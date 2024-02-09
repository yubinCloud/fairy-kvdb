package fairy_kvdb

import (
	"errors"
	"fairy-kvdb/data"
	"fairy-kvdb/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读取
	index      index.Indexer
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
	// 初始化索引
	idx := index.NewIndexer(index.TypeEnum(options.IndexType))
	// 初始化数据库实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      idx,
	}
	// 加载数据文件
	fileIds, err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}
	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(fileIds); err != nil {
		return nil, err
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
	}
	// 将 LogRecord 写入到数据文件中
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 将 LogRecordPos 更新到内存索引中
	if ok := db.index.Put(key, pos); !ok {
		return ErrorIndexUpdateFailed
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
	}
	// 将 LogRecord 写入到数据文件中
	_, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 将 key 从内存索引中删除
	if ok := db.index.Delete(key); !ok {
		return ErrorIndexUpdateFailed
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
	db.mu.Lock()
	defer db.mu.Unlock()

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

	// 根据用户配置的持久化策略，将 LogRecordPos 持久化到磁盘中
	if db.options.SyncEveryWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 返回 LogRecordPos
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
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
	dataFile, err := data.OpenDataFile(db.options.DataDir, initialFid)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

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
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DataDir, fileId)
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

// 从数据文件中加载索引
// 遍历所有的数据文件，将 LogRecordPos 加载到内存索引中
func (db *DB) loadIndexFromDataFiles(fileIds []uint32) error {
	// 如果没有数据文件，则直接返回
	if len(fileIds) == 0 {
		return nil
	}
	// 遍历所有的数据文件
	for _, fid := range fileIds {
		var dataFile *data.DataFile
		// fid -> dataFile
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}
		// load index from one data file
		offset, err := db.loadIndexFromOneDataFile(dataFile)
		if err != nil {
			return err
		}
		// 如果当前是活跃文件，则更新 WriteOffset
		if fid == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}

func (db *DB) loadIndexFromOneDataFile(dataFile *data.DataFile) (int64, error) {
	var offset int64 = 0
	for {
		record, length, err := dataFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return offset, err
		}
		pos := data.LogRecordPos{Fid: dataFile.FileId, Offset: offset}
		var ok bool
		if record.Type == data.LogRecordNormal {
			ok = db.index.Put(record.Key, &pos)
		} else {
			ok = db.index.Delete(record.Key)
		}
		if !ok {
			return offset, ErrorIndexUpdateFailed
		}
		// 移动 offset
		offset += length
	}
	return offset, nil
}

func checkOptions(options *Options) error {
	if options.DataDir == "" {
		return errors.New("data dir is empty")
	}
	if options.MaxFileSize <= 0 {
		return errors.New("max file size is invalid")
	}
	return nil
}
