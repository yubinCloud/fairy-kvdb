package fairy_kvdb

import (
	"errors"
	"fairy-kvdb/data"
	"fairy-kvdb/fio"
	"fairy-kvdb/index"
	"fairy-kvdb/utils"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const fileLockName = "fairy-kvdb.lock"

// DB 存储引擎实例
type DB struct {
	options        Options
	mu             *sync.RWMutex
	activeFile     *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles     map[uint32]*data.DataFile // 旧的数据文件，只能用于读取
	index          index.Indexer
	nextBTSN       uint64       // 下一个 Batch Transaction Sequence Number，全局递增
	isMerging      int32        // 是否正在执行 merge 操作（0 表示 false，1 表示 true）
	btsnFileExists bool         // 标识 btsn file 是否存在
	isPureBoot     bool         // 是否是纯净启动，也就是启动时数据目录下没有任何数据
	fileLock       *flock.Flock // 文件锁，保证多进程之间的互斥
	bytesWrite     uint64       // 在数据文件中累计写了多少字节（用于决定什么时候同步）
	reclaimSize    uint64       // 表示有多少数据是无效的，可以用于决定什么时候进行 merge
}

type Stat struct {
	KeyNum          uint   `json:"keyNum"`          // key 的总数量
	DataFileNum     uint   `json:"dataFileNum"`     // 数据文件的数量
	ReclaimableSize uint64 `json:"reclaimableSize"` // 可以进行 merge 回收的数据量，以字节为单位
	DiskSize        int64  `json:"diskSize"`        // 数据目录所占磁盘空间的大小
}

// Open 打开存储引擎实例
func Open(options Options) (*DB, error) {
	// 检查配置项
	if err := checkOptions(&options); err != nil {
		return nil, err
	}
	// 判断数据目录是否存在，如果不存在则创建这个目录
	if _, err := os.Stat(options.DataDir); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DataDir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 判断本次数据库启动是否属于 pure boot，也就是数据目录中没有任何文件
	isPureBoot := false
	dirEntries, err := os.ReadDir(options.DataDir)
	if err != nil {
		return nil, err
	}
	if len(dirEntries) == 0 {
		isPureBoot = true
	}
	// 判断当前数据目录是否正在使用（使用 flock）
	fileLock := flock.New(filepath.Join(options.DataDir, fileLockName))
	holdFileLock, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !holdFileLock {
		return nil, ErrorDatabaseIsUsing
	}
	// 初始化索引
	idx := index.NewIndexer(index.TypeEnum(options.IndexType), options.BPlusTreeIndexOpts)
	// 初始化数据库实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      idx,
		nextBTSN:   1,
		isPureBoot: isPureBoot,
		fileLock:   fileLock,
		bytesWrite: 0,
	}
	// 在加载数据文件之前，先加载 merge 目录的文件，将 merge 的结果先合并到数据文件目录中
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载数据文件
	fileIds, err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	if options.IndexType != int8(index.BPlusTreeIndexer) { // B+树不需要从数据文件加载索引
		// 先从 Hint 文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(fileIds); err != nil {
			return nil, err
		}
		// 如果采用 mmap 加载数据文件，那么需要在完成加载后将所加载的文件变为 StandardIO
		if options.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}
	} else {
		// B+树索引需要从 btsn file 中取出保存的 NextBTSN 值
		if err := db.loadNextBSTN(); err != nil {
			return nil, err
		}
		// 恢复 activeFile 的 WriteOffset
		if db.activeFile != nil {
			if sz, err := db.activeFile.IoManger.Size(); err != nil {
				return nil, err
			} else {
				db.activeFile.WriteOffset = sz
			}

		}
	}

	return db, nil
}

// Put 写入 key-value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	// 构造 LogRecord 结构体
	record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
		Btsn:  data.NoTxnBTSN,
	}
	// 将 LogRecord 写入到数据文件中
	db.mu.Lock()
	defer db.mu.Unlock()
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 将 LogRecordPos 更新到内存索引中
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.increaseReclaimSize(oldPos.Sz)
	}

	return nil
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	// 检查 key 是否存在，不存在则直接返回
	if pos := db.index.Get(key); pos == nil {
		return ErrorKeyNotFound
	}
	// 构造 LogRecord 结构体
	record := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
		Btsn: data.NoTxnBTSN,
	}
	// 将 LogRecord 写入到数据文件中
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 将 key 从内存索引中删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrorIndexUpdateFailed
	}
	if oldPos != nil {
		db.increaseReclaimSize(oldPos.Sz)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrorKeyEmpty
	}

	// 从内存索引中获取 LogRecordPos
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrorKeyNotFound
	}

	record, err := db.readLogRecord(pos)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

// ListKeys 返回所有的 key
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	defer iter.Close()
	keys := make([][]byte, db.index.Size())
	idx := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys
}

// Fold 遍历所有的 key-value 数据，并执行用户指定的操作，函数返回 false 时终止
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.index.Iterator(false)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		pos := iter.Value()
		record, err := db.readLogRecord(pos)
		if err != nil {
			return err
		}
		if !fn(record.Key, record.Value) {
			break
		}
	}
	return nil
}

// Close 关闭存储引擎实例
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 保存当前事务的序列号
	btsnFile, err := data.OpenBtsnFile(db.options.DataDir)
	if err != nil {
		return err
	}
	err = btsnFile.WriteBtsnRecord(db.nextBTSN)
	if err != nil {
		return err
	}
	err = btsnFile.Sync()
	if err != nil {
		return err
	}
	err = btsnFile.Close()
	if err != nil {
		return err
	}
	// 关闭所有的数据文件
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}
	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return err
		}
	}
	// 关闭 index
	if err = db.index.Close(); err != nil {
		return err
	}
	// 关闭 fileLock
	if err = db.fileLock.Unlock(); err != nil {
		panic(fmt.Sprintf("failed to unlock the directory, %v", err))
	}
	return nil
}

// Sync 将数据持久化到磁盘中
func (db *DB) Sync() error {
	if db.activeFile != nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat 计算数据库统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 计算 dataFileNum
	dataFileNum := uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileNum++
	}
	// 计算 dirSize
	dirSize, err := utils.DirSize(db.options.DataDir)
	if err != nil {
		panic(fmt.Sprintf("failed to get the size of the directory, %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFileNum,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// CopyBackup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) CopyBackup(backupDir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	excludes := []string{fileLockName} // 需要排除的拷贝文件
	return utils.CopyDir(db.options.DataDir, backupDir, excludes)
}

// FetchNextBTSN 获取下一个 BTSN
func (db *DB) FetchNextBTSN() uint64 {
	nextLSN := atomic.AddUint64(&db.nextBTSN, 1)
	return nextLSN
}

// readLogRecord 根据 LogRecordPos 读取 LogRecord
func (db *DB) readLogRecord(pos *data.LogRecordPos) (*data.LogRecord, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	// 如果数据文件不存在，则直接返回错误
	if dataFile == nil {
		return nil, ErrorDataFileNotFound
	}
	// 根据 offset 读取数据
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// 追加数据到活跃文件末尾
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前的活跃文件是否存在，因为数据库在没有写入数据的时候是没有文件生成的
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 对 LogRecord 进行序列化
	encoded, length := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOffset+length > db.options.MaxFileSize {
		// 先持久化数据文件，保证已有的数据持久化到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前的活跃文件加入到旧文件中
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// 打开一个新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 将 LogRecord 写入到活跃文件中
	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encoded); err != nil {
		return nil, err
	}
	db.bytesWrite += uint64(length)

	// 根据用户配置的持久化策略，将 LogRecordPos 持久化到磁盘中
	needSync := db.options.SyncEveryWrite || db.bytesWrite > db.options.BytesPerSync
	if needSync {
		db.bytesWrite = 0 // 清空累计值
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 返回 LogRecordPos
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
		Sz:     uint64(length),
	}
	return pos, nil
}

// 设置当前的活跃文件
// 访问这个方法前必须加锁
func (db *DB) setActiveFile() error {
	var initialFid uint32 = 0
	if db.activeFile != nil {
		initialFid = db.activeFile.FileId + 1
	}
	// 打开一个新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DataDir, initialFid, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载数据文件，并所有文件打开，并保存 fileId
func (db *DB) loadDataFiles() (fileIds []uint32, err error) {
	dirEntries, err := os.ReadDir(db.options.DataDir)
	if err != nil {
		return fileIds, err
	}

	// 遍历数据目录下的文件，找到所有以 `.data` 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.NameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return fileIds, ErrorDataFileCorrupt
			}
			fileIds = append(fileIds, uint32(fileId))
		}
	}

	// 对文件 ID 进行排序，从小到大依次加载
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})

	// 遍历文件 ID，加载数据文件
	ioType := fio.StandardFIO
	if db.options.MMapAtStartup {
		ioType = fio.MemoryMapIO
	}
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DataDir, fileId, ioType)
		if err != nil {
			return fileIds, err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fileId] = dataFile
		}
	}

	return fileIds, nil
}

// 数据库在打开时加载过程的上下文
type dbOpenLoadingContext struct {
	batchTxns map[uint64][]data.BatchTxnRecord
	maxBtsn   uint64
}

// 从数据文件中加载索引
// 遍历所有的数据文件，将 LogRecordPos 加载到内存索引中
func (db *DB) loadIndexFromDataFiles(fileIds []uint32) error {
	// 如果没有数据文件，则直接返回
	if len(fileIds) == 0 {
		return nil
	}
	// 先查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DataDir, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		hasMerge = true
		nonMergeFileId, err = db.getNonMergeFileId(db.options.DataDir)
		if err != nil {
			return err
		}
	}
	loadContext := dbOpenLoadingContext{
		batchTxns: make(map[uint64][]data.BatchTxnRecord),
		maxBtsn:   0,
	}
	// 遍历所有的数据文件
	for _, fid := range fileIds {
		// 首先与 nonMergeFileId 进行比较，如果当前文件 ID 小于 nonMergeFileId，则直接跳过，因为已经通过 Hint 文件加载过了
		if hasMerge && fid < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		// fid -> dataFile
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}
		// load index from one data file
		offset, err := db.loadIndexFromOneDataFile(dataFile, &loadContext)
		if err != nil {
			return err
		}
		// 如果当前是活跃文件，则更新 WriteOffset
		if fid == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
	}
	// 更新 db 的 btsn
	db.nextBTSN = loadContext.maxBtsn + 1
	return nil
}

func (db *DB) loadIndexFromOneDataFile(dataFile *data.DataFile, loadContext *dbOpenLoadingContext) (int64, error) {
	var offset int64 = 0
	for {
		record, length, err := dataFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return offset, err
		}
		// 先更新 BTSN
		btsn := record.Btsn
		if btsn > loadContext.maxBtsn {
			loadContext.maxBtsn = btsn
		}

		if record.Btsn == data.NoTxnBTSN { // 对于非 batch txn 操作，则直接更新索引
			pos := data.LogRecordPos{Fid: dataFile.FileId, Offset: offset}
			ok := db.redoLogRecord(record, &pos)
			if !ok {
				return offset, ErrorIndexUpdateFailed
			}
		} else { // 对于 batch txn 操作，则根据是否为 End 来决定 redo 还是暂存
			batchTxns := loadContext.batchTxns
			if record.Type == data.LogRecordBatchEnd {
				txnRecords := batchTxns[btsn]
				for _, txnRecord := range txnRecords {
					db.redoLogRecord(txnRecord.Record, txnRecord.Pos)
				}
				delete(batchTxns, btsn)
			} else {
				batchTxns[btsn] = append(batchTxns[btsn], data.BatchTxnRecord{
					Record: record,
					Pos:    &data.LogRecordPos{Fid: dataFile.FileId, Offset: offset},
				})
			}
		}
		// 移动 offset
		offset += length
	}
	return offset, nil
}

func (db *DB) redoLogRecord(record *data.LogRecord, pos *data.LogRecordPos) bool {
	if record.Type == data.LogRecordNormal {
		oldPos := db.index.Put(record.Key, pos)
		if oldPos != nil {
			db.increaseReclaimSize(oldPos.Sz)
		}
		return true
	} else if record.Type == data.LogRecordDelete {
		oldPos, _ := db.index.Delete(record.Key)
		if oldPos != nil {
			db.increaseReclaimSize(oldPos.Sz)
		}
		return true
	}
	return false
}

// 检查配置项
func checkOptions(options *Options) error {
	if options.DataDir == "" {
		return errors.New("data dir is empty")
	}
	if options.MaxFileSize <= 0 {
		return errors.New("max file size is invalid")
	}
	if options.MergeRatio < 0 || options.MergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

// 从 BTSN file 中加载 NextBTSN 值
func (db *DB) loadNextBSTN() error {
	path := filepath.Join(db.options.DataDir, data.BtsnFileName)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		db.btsnFileExists = false
		return nil
	}
	db.btsnFileExists = true
	btsnFile, err := data.OpenBtsnFile(db.options.DataDir)
	if err != nil {
		return err
	}
	defer func() {
		_ = btsnFile.Close()
		_ = os.Remove(path)
	}()
	bstn, err := btsnFile.ReadBtsnRecord()
	if err != nil {
		return err
	}
	db.nextBTSN = bstn
	return nil
}

// 将数据文件的 IO 类型设置为 standard io
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	// 将 activeFile 转为 StandardIO
	if err := db.activeFile.SetIOManager(db.options.DataDir, fio.StandardFIO); err != nil {
		return err
	}
	// 将 old files 转为 StandardIO
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DataDir, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) increaseReclaimSize(sz uint64) {
	atomic.AddUint64(&db.reclaimSize, sz)
}
