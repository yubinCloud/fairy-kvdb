package index

import (
	"bytes"
	"fairy-kvdb/data"
	"github.com/google/btree"
)

// IndexItem 在 index 中存储的数据元素
type IndexItem struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *IndexItem) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*IndexItem).key) == 1
}
