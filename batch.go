package fairy_kvdb

import (
	"fairy-kvdb/data"
	"sync"
)

// WriteBatch 用于批量写入数据
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       options,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 将 LogRecord 暂存到 pendingWrites 中
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
	}
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	keyString := string(key)
	// 如果数据不存在，则直接返回
	pos := wb.db.index.Get(key)
	if pos == nil {
		if wb.pendingWrites[keyString] != nil {
			delete(wb.pendingWrites, keyString)
		}
		return nil
	}
	// 将 LogRecord 暂存到 pendingWrites 中
	wb.pendingWrites[keyString] = &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}
	return nil
}

// Commit 提交事务，将暂存的数据写道数据文件，并更新索引
func (wb *WriteBatch) Commit() error {
	// 为 WriteBatch 加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 校验 pendingWrites
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if len(wb.pendingWrites) > wb.options.MaxBatchNum {
		return ErrorExceedMaxWriteBatchNum
	}
	// 为 db 加锁，保证 txn 提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 获取当前最新 txn 的 BTSN
	btsn := wb.db.FetchNextBTSN()
	// 写数据到数据文件中
	positions := make(map[*data.LogRecord]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		record.Btsn = btsn
		pos, err := wb.db.appendLogRecord(record)
		if err != nil {
			return err
		}
		positions[record] = pos
		// 更新索引
		if record.Type == data.LogRecordDelete {
			wb.db.index.Delete(record.Key)
		} else {
			wb.db.index.Put(record.Key, pos)
		}
	}
	// 写一条标识事务完成的数据
	endRecord := &data.LogRecord{
		Key:   make([]byte, 0),
		Value: make([]byte, 0),
		Type:  data.LogRecordBatchEnd,
		Btsn:  btsn,
	}
	if _, err := wb.db.appendLogRecord(endRecord); err != nil {
		return err
	}
	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	// 更新内存索引
	for record, pos := range positions {
		if record.Type == data.LogRecordDelete {
			wb.db.index.Delete(record.Key)
		} else {
			wb.db.index.Put(record.Key, pos)
		}
	}
	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}
