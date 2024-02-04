package index

import (
	"bytes"
	"fairy-kvdb/data"
	"github.com/google/btree"
	"sync"
)

// BTreeItem 在 Google 的 btree 数据结构中存储的数据元素
type BTreeItem struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *BTreeItem) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*BTreeItem).key) == 1
}

// BTree btree index，封装了 Google 的 btree kv
type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex // tree 的访问是并发不安全的，因此需要对 tree 的写操作进行并发控制
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	itm := &BTreeItem{
		key: key,
		pos: pos,
	}
	bt.mu.Lock()
	bt.tree.ReplaceOrInsert(itm)
	bt.mu.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	query := &BTreeItem{
		key: key,
	}
	bt.mu.RLock()
	itm := bt.tree.Get(query)
	bt.mu.RUnlock()
	if itm == nil {
		return nil
	}
	return itm.(*BTreeItem).pos
}

func (bt *BTree) Delete(key []byte) bool {
	query := &BTreeItem{key: key}
	bt.mu.Lock()
	oldItem := bt.tree.Delete(query)
	bt.mu.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
