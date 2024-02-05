package fairy_kvdb

import (
	"fairy-kvdb/data"
	"fairy-kvdb/index"
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

	// 根据文件 ID 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	// 如果数据文件不存在，则返回错误
	if dataFile == nil {
		return nil, ErrorDataFileNotFound
	}
	// 根据 offset 读取数据
	record, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
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
	db.activeFile.WriteOffset += length

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
