package index

import (
	"fairy-kvdb/data"
	"fairy-kvdb/index"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTreeIndex_Basic(t *testing.T) {
	// 创建一个 ART 索引
	artIndex := index.NewAdaptiveRadixTreeIndex()
	// 向索引中存储 key 对应的数据位置信息
	artIndex.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	artIndex.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 15})
	// case: normal Get
	pos := artIndex.Get([]byte("key-1"))
	assert.NotNil(t, pos)
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(12), pos.Offset)
	// case: normal Size
	size := artIndex.Size()
	assert.Equal(t, 2, size)
	// case：Get 不存在的数据
	pos2 := artIndex.Get([]byte("key-non"))
	assert.Nil(t, pos2)
	// case: 重新 put
	artIndex.Put([]byte("key-1"), &data.LogRecordPos{Fid: 2, Offset: 13})
	pos3 := artIndex.Get([]byte("key-1"))
	assert.NotNil(t, pos3)
	assert.Equal(t, uint32(2), pos3.Fid)
	assert.Equal(t, int64(13), pos3.Offset)
	// case: 删除 key
	ok := artIndex.Delete([]byte("key-1"))
	assert.True(t, ok)
	pos4 := artIndex.Get([]byte("key-1"))
	assert.Nil(t, pos4)
	pos5 := artIndex.Get([]byte("key-2"))
	assert.NotNil(t, pos5)
	assert.Equal(t, uint32(1), pos5.Fid)
	assert.Equal(t, int64(15), pos5.Offset)
	assert.Equal(t, 1, artIndex.Size())
	// case: 重复删除
	ok2 := artIndex.Delete([]byte("key-1"))
	assert.False(t, ok2)
}

func TestAdaptiveRadixTreeIndex_Iterator(t *testing.T) {
	// 创建一个 ART 索引
	artIndex := index.NewAdaptiveRadixTreeIndex()
	// 向索引中存储 key 对应的数据位置信息
	artIndex.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	artIndex.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 15})
	artIndex.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 18})
	artIndex.Put([]byte("key-4"), &data.LogRecordPos{Fid: 1, Offset: 20})
	// case: 正序遍历
	iter := artIndex.Iterator(false)
	assert.NotNil(t, iter)
	iter.Rewind()
	iter.Seek([]byte("key-2"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key-2"), iter.Key())
	assert.Equal(t, int64(15), iter.Value().Offset)
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key-3"), iter.Key())
	assert.Equal(t, int64(18), iter.Value().Offset)
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key-4"), iter.Key())
	assert.Equal(t, int64(20), iter.Value().Offset)
	iter.Next()
	assert.False(t, iter.Valid())
	// case: 反序遍历
	iter2 := artIndex.Iterator(true)
	assert.NotNil(t, iter2)
	iter2.Seek([]byte("key-2"))
	assert.True(t, iter2.Valid())
	assert.Equal(t, []byte("key-2"), iter2.Key())
	assert.Equal(t, int64(15), iter2.Value().Offset)
	iter2.Next()
	assert.True(t, iter2.Valid())
	assert.Equal(t, []byte("key-1"), iter2.Key())
	assert.Equal(t, int64(12), iter2.Value().Offset)
	iter2.Next()
	assert.False(t, iter2.Valid())
}
