package fairy_kvdb

import (
	"bytes"
	"fairy-kvdb/index"
)

// Iterator 数据库层面的迭代器
type Iterator struct {
	indexIterator index.Iterator // 索引迭代器
	db            *DB
	options       IteratorOptions // 迭代器选项
}

func (db *DB) NewIterator(options *IteratorOptions) *Iterator {
	return &Iterator{
		indexIterator: db.index.Iterator(options.Reverse),
		db:            db,
		options:       *options,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (iter *Iterator) Rewind() {
	iter.indexIterator.Rewind()
	iter.skipToNext() // 跳过 prefix 不满足条件的 key
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (iter *Iterator) Seek(key []byte) {
	iter.indexIterator.Seek(key)
	iter.skipToNext() // 跳过 prefix 不满足条件的 key
}

func (iter *Iterator) Next() {
	iter.indexIterator.Next()
	iter.skipToNext() // 跳过 prefix 不满足条件的 key
}

func (iter *Iterator) Valid() bool {
	return iter.indexIterator.Valid()
}

func (iter *Iterator) Key() []byte {
	return iter.indexIterator.Key()
}

func (iter *Iterator) Value() []byte {
	recordPos := iter.indexIterator.Value()
	iter.db.mu.RLock()
	defer iter.db.mu.RUnlock()
	record, err := iter.db.readLogRecord(recordPos)
	if err != nil {
		return nil
	}
	return record.Value
}

func (iter *Iterator) Close() {
	iter.indexIterator.Close()
}

func (iter *Iterator) skipToNext() {
	prefixLen := len(iter.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; iter.indexIterator.Valid(); iter.indexIterator.Next() {
		if bytes.HasPrefix(iter.indexIterator.Key(), iter.options.Prefix) {
			break
		}
	}
}
