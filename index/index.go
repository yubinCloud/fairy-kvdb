package index

import (
	"fairy-kvdb/data"
)

// Indexer abstract index interface
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除 key
	Delete(key []byte) bool
}
