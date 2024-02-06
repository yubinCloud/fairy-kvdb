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

type TypeEnum int8

const (
	BTreeIndex TypeEnum = iota // BTree 索引
	ARTIndex                   // ART 自适应基数树索引

)

// NewIndexer 根据类型初始化索引
func NewIndexer(indexType TypeEnum) Indexer {
	switch indexType {
	case BTreeIndex:
		return NewBTree()
	case ARTIndex:
		return nil
	default:
		panic("unknown index type")
	}
}
