package index

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/data"
	"fairy-kvdb/index"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTreeIndex_Basic(t *testing.T) {
	// 创建 index
	dirPath := filepath.Join(fairydb.DefaultOptions.DataDir, "bptree")
	_ = os.RemoveAll(dirPath)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		_ = os.MkdirAll(dirPath, os.ModePerm)
	}
	options := &index.BPlusTreeIndexOptions{
		BboltOptions: bbolt.DefaultOptions,
		DataDir:      dirPath,
	}
	bpt := index.NewBPlusTreeIndex(options)
	defer func() {
		_ = os.RemoveAll(dirPath)
	}()
	// 插入数据
	const count = 10
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		bpt.Put(key, &data.LogRecordPos{Fid: uint32(i), Offset: int64(i * 10)})
	}
	// case: Get
	for i := 10; i < count+5; i++ {
		pos := bpt.Get([]byte(fmt.Sprintf("key%d", i)))
		if i < count {
			assert.NotNil(t, pos)
			assert.Equal(t, uint32(i), pos.Fid)
			assert.Equal(t, int64(i*10), pos.Offset)
		} else {
			assert.Nil(t, pos)
		}
	}
	// case: Size
	assert.Equal(t, count, bpt.Size())
	// case: Delete
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			ok := bpt.Delete([]byte(fmt.Sprintf("key%d", i)))
			assert.True(t, ok)
		}
	}
	for i := 0; i < count; i++ {
		pos := bpt.Get([]byte(fmt.Sprintf("key%d", i)))
		if i%2 == 0 {
			assert.Nil(t, pos)
		} else {
			assert.NotNil(t, pos)
			assert.Equal(t, uint32(i), pos.Fid)
			assert.Equal(t, int64(i*10), pos.Offset)
		}
	}
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			ok := bpt.Delete([]byte(fmt.Sprintf("key%d", i)))
			assert.False(t, ok)
		}
	}
	assert.Equal(t, count/2, bpt.Size())
}

func TestBPlusTreeIndex_Iterator(t *testing.T) {
	// 创建 index
	dirPath := filepath.Join(fairydb.DefaultOptions.DataDir, "bptree")
	_ = os.RemoveAll(dirPath)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		_ = os.MkdirAll(dirPath, os.ModePerm)
	}
	options := &index.BPlusTreeIndexOptions{
		BboltOptions: bbolt.DefaultOptions,
		DataDir:      dirPath,
	}
	bpt := index.NewBPlusTreeIndex(options)
	defer func() {
		_ = os.RemoveAll(dirPath)
	}()
	// 插入数据
	const count = 10
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		bpt.Put(key, &data.LogRecordPos{Fid: uint32(i), Offset: int64(i * 10)})
	}
	// 正向 iterator
	iter := bpt.Iterator(false)
	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		assert.Equal(t, []byte(fmt.Sprintf("key%d", i)), key)
		pos := iter.Value()
		assert.Equal(t, uint32(i), pos.Fid)
		assert.Equal(t, int64(i*10), pos.Offset)
		i++
	}
	iter.Close()
	// 反向 iterator
	iter2 := bpt.Iterator(true)
	i = count - 1
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		key := iter2.Key()
		assert.Equal(t, []byte(fmt.Sprintf("key%d", i)), key)
		pos := iter2.Value()
		assert.Equal(t, uint32(i), pos.Fid)
		assert.Equal(t, int64(i*10), pos.Offset)
		i--
	}
	iter2.Close()
}
