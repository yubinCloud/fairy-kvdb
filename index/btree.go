package index

import (
	"bytes"
	"fairy-kvdb/data"
	"github.com/google/btree"
	"sort"
	"sync"
)

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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	itm := &IndexItem{
		key: key,
		pos: pos,
	}
	bt.mu.Lock()
	oldItem := bt.tree.ReplaceOrInsert(itm)
	bt.mu.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*IndexItem).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	query := &IndexItem{
		key: key,
	}
	bt.mu.RLock()
	itm := bt.tree.Get(query)
	bt.mu.RUnlock()
	if itm == nil {
		return nil
	}
	return itm.(*IndexItem).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	query := &IndexItem{key: key}
	bt.mu.Lock()
	oldItem := bt.tree.Delete(query)
	bt.mu.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*IndexItem).pos, true
}

func (bt *BTree) Size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return NewBTreeIterator(bt, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BTreeIterator BTree 索引迭代器
type BTreeIterator struct {
	currIndex int          // 当前遍历的下标位置
	reverse   bool         // 是否是逆序遍历
	values    []*IndexItem // key + 位置索引信息
}

// NewBTreeIterator 初始化 BTree 索引迭代器
func NewBTreeIterator(bt *BTree, reverse bool) *BTreeIterator {
	var idx int
	values := make([]*IndexItem, bt.tree.Len())

	// 将 btree 索引中的数据都取出来
	saveValuesHandler := func(item btree.Item) bool {
		values[idx] = item.(*IndexItem)
		idx++
		return true
	}
	if !reverse {
		bt.tree.Descend(saveValuesHandler)
	} else {
		bt.tree.Ascend(saveValuesHandler)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (iter *BTreeIterator) Rewind() {
	iter.currIndex = 0
}

func (iter *BTreeIterator) Seek(key []byte) {
	// 二分查找（因为从 B+ Tree 中取出来的数据已经是有序的了）
	comparator := func(i int) bool {
		return bytes.Compare(iter.values[i].key, key) >= 0
	}
	if iter.reverse {
		comparator = func(i int) bool {
			return bytes.Compare(iter.values[i].key, key) <= 0
		}
	}
	iter.currIndex = sort.Search(len(iter.values), comparator)
}

func (iter *BTreeIterator) Next() {
	iter.currIndex++
}

func (iter *BTreeIterator) Valid() bool {
	return iter.currIndex < len(iter.values)
}

func (iter *BTreeIterator) Key() []byte {
	return iter.values[iter.currIndex].key
}

func (iter *BTreeIterator) Value() *data.LogRecordPos {
	return iter.values[iter.currIndex].pos
}

func (iter *BTreeIterator) Close() {
	iter.currIndex = 0
	iter.values = nil
}
