package index

import (
	"fairy-kvdb/data"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const bboltEngineFilename = "bbolt.db"
const indexBucketName = "fairydb-index"

// BPlusTreeIndex B+ Tree 索引
type BPlusTreeIndex struct {
	tree *bbolt.DB // 封装了 bbolt 数据库引擎来实现，是并发安全的，因此不需要再加锁
}

// BPlusTreeIndexOptions B+ Tree 索引配置项
type BPlusTreeIndexOptions struct {
	BboltOptions *bbolt.Options
	DataDir      string
}

// NewBPlusTreeIndex 初始化 B+ Tree 索引
func NewBPlusTreeIndex(options *BPlusTreeIndexOptions) *BPlusTreeIndex {
	if _, err := os.Stat(options.DataDir); os.IsNotExist(err) {
		_ = os.MkdirAll(options.DataDir, os.ModePerm)
	}
	bpTree, err := bbolt.Open(filepath.Join(options.DataDir, bboltEngineFilename), 0644, options.BboltOptions)
	if err != nil {
		panic("failed to open bpTree file")
	}
	// 创建对应的 bucket
	if err = bpTree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(indexBucketName))
		return err
	}); err != nil {
		panic("failed to create bucket in bpTree")
	}
	return &BPlusTreeIndex{
		tree: bpTree,
	}
}

func (bpt *BPlusTreeIndex) Put(key []byte, pos *data.LogRecordPos) bool {
	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(indexBucketName))
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	})
	if err != nil {
		panic("failed to put data into bpTree")
	}
	return true
}

func (bpt *BPlusTreeIndex) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(indexBucketName))
		rawValue := bucket.Get(key)
		if len(rawValue) != 0 {
			pos = data.DecodeLogRecordPos(rawValue)
		}
		return nil
	})
	if err != nil {
		panic("failed to get data from bpTree")
	}
	return pos
}

func (bpt *BPlusTreeIndex) Delete(key []byte) bool {
	isExists := false
	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(indexBucketName))
		rawValue := bucket.Get(key) // 注意这里需要将 Get 和下面的 Delete 放在一个 txn 中执行
		if len(rawValue) != 0 {
			isExists = true
			return bucket.Delete(key)
		}
		return nil
	})
	if err != nil {
		panic("failed to delete data from bpTree")
	}
	return isExists
}

func (bpt *BPlusTreeIndex) Size() int {
	size := 0
	err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(indexBucketName))
		size = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		panic("failed to get size from bpTree")
	}
	return size
}

func (bpt *BPlusTreeIndex) Iterator(reverse bool) Iterator {
	return NewBPlusTreeIterator(bpt, reverse)
}

func (bpt *BPlusTreeIndex) Close() error {
	return bpt.tree.Close()
}

// BPlusTreeIterator B+Tree 索引迭代器
type BPlusTreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func NewBPlusTreeIterator(bpt *BPlusTreeIndex, reverse bool) *BPlusTreeIterator {
	tx, err := bpt.tree.Begin(false) // 手动开启一个事务
	if err != nil {
		panic("failed to begin transaction in bpTree")
	}
	cursor := tx.Bucket([]byte(indexBucketName)).Cursor()
	iter := &BPlusTreeIterator{
		tx:      tx,
		cursor:  cursor,
		reverse: reverse,
	}
	iter.Rewind()
	return iter
}

func (iter *BPlusTreeIterator) Rewind() {
	if iter.reverse {
		iter.currKey, iter.currValue = iter.cursor.Last()
	} else {
		iter.currKey, iter.currValue = iter.cursor.First()
	}
}

func (iter *BPlusTreeIterator) Seek(key []byte) {
	iter.currKey, iter.currValue = iter.cursor.Seek(key)
}

func (iter *BPlusTreeIterator) Next() {
	if iter.reverse {
		iter.currKey, iter.currValue = iter.cursor.Prev()
	} else {
		iter.currKey, iter.currValue = iter.cursor.Next()
	}
}

func (iter *BPlusTreeIterator) Valid() bool {
	return len(iter.currKey) != 0
}

func (iter *BPlusTreeIterator) Key() []byte {
	return iter.currKey
}

func (iter *BPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(iter.currValue)
}

func (iter *BPlusTreeIterator) Close() {
	_ = iter.tx.Rollback() // 对于只读事务，只需要 rollback 就可以
}
