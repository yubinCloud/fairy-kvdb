package index

import (
	"bytes"
	"fairy-kvdb/data"
	gart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTreeIndex 自适应基数树索引
type AdaptiveRadixTreeIndex struct {
	tree gart.Tree
	mu   *sync.RWMutex
}

// NewAdaptiveRadixTreeIndex 初始化 ART 索引
func NewAdaptiveRadixTreeIndex() *AdaptiveRadixTreeIndex {
	return &AdaptiveRadixTreeIndex{
		tree: gart.New(),
		mu:   new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTreeIndex) Put(key []byte, pos *data.LogRecordPos) bool {
	art.mu.Lock()
	art.tree.Insert(key, pos)
	art.mu.Unlock()
	return true
}

func (art *AdaptiveRadixTreeIndex) Get(key []byte) *data.LogRecordPos {
	art.mu.RLock()
	pos, found := art.tree.Search(key)
	art.mu.RUnlock()
	if !found {
		return nil
	}
	return pos.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTreeIndex) Delete(key []byte) bool {
	art.mu.Lock()
	_, ok := art.tree.Delete(key)
	art.mu.Unlock()
	return ok
}

func (art *AdaptiveRadixTreeIndex) Size() int {
	art.mu.RLock()
	size := art.tree.Size()
	art.mu.RUnlock()
	return size
}

func (art *AdaptiveRadixTreeIndex) Iterator(reverse bool) Iterator {
	art.mu.RLock()
	iter := newArtIterator(art.tree, reverse)
	art.mu.RUnlock()
	return iter
}

func (art *AdaptiveRadixTreeIndex) Close() error {
	return nil
}

// ArtIterator ART 索引迭代器
type ArtIterator struct {
	currIndex int
	reverse   bool
	values    []*IndexItem
}

func newArtIterator(tree gart.Tree, reverse bool) *ArtIterator {
	values := make([]*IndexItem, tree.Size())
	idx := 0
	if reverse {
		idx = tree.Size() - 1
	}
	// 将所有数据存放到 values 中
	tree.ForEach(func(node gart.Node) bool {
		values[idx] = &IndexItem{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	})
	return &ArtIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (iter *ArtIterator) Rewind() {
	iter.currIndex = 0
}

func (iter *ArtIterator) Seek(key []byte) {
	if iter.reverse {
		iter.currIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].key, key) <= 0
		})
	} else {
		iter.currIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].key, key) >= 0
		})
	}
}

func (iter *ArtIterator) Next() {
	iter.currIndex++
}

func (iter *ArtIterator) Valid() bool {
	return iter.currIndex < len(iter.values)
}

func (iter *ArtIterator) Key() []byte {
	return iter.values[iter.currIndex].key
}

func (iter *ArtIterator) Value() *data.LogRecordPos {
	return iter.values[iter.currIndex].pos
}

func (iter *ArtIterator) Close() {
	iter.values = nil
}
