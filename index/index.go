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

	// Size 返回索引中的数据量
	Size() int

	// Iterator 返回一个迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

type TypeEnum int8

const (
	BTreeIndexer     TypeEnum = iota // BTree 索引
	ARTIndexer                       // ART 自适应基数树索引
	BPlusTreeIndexer                 // B+Tree 索引
)

// NewIndexer 根据类型初始化索引
func NewIndexer(indexType TypeEnum, bptOptions *BPlusTreeIndexOptions) Indexer {
	switch indexType {
	case BTreeIndexer:
		return NewBTree()
	case ARTIndexer:
		return NewAdaptiveRadixTreeIndex()
	case BPlusTreeIndexer:
		return NewBPlusTreeIndex(bptOptions)
	default:
		panic("unknown index type")
	}
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 判断迭代器是否有效，即是否已经遍历完了
	Valid() bool

	// Key 当前遍历位置的 key 数据
	Key() []byte

	// Value 当前遍历位置的 value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器
	Close()
}
