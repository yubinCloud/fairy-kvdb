package index

import (
	"fairy-kvdb/data"
	"fairy-kvdb/index"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := index.NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := index.NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	assert.Equal(t, int64(2), res3.Offset)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := index.NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok := bt.Delete(nil)
	assert.NotNil(t, res2)
	assert.Equal(t, int64(100), res2.Offset)
	assert.True(t, ok)

	KEY2 := []byte("a")
	res3 := bt.Put(KEY2, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res3)
	res4, ok := bt.Delete(KEY2)
	assert.NotNil(t, res4)
	assert.Equal(t, int64(100), res4.Offset)
	assert.True(t, ok)

	res5, ok := bt.Delete([]byte("ccc"))
	assert.Nil(t, res5)
	assert.False(t, ok)
}

func TestBTree_Size(t *testing.T) {
	bt := index.NewBTree()
	assert.Equal(t, 0, bt.Size())

	bt.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Equal(t, 1, bt.Size())

	bt.Put([]byte("bb"), &data.LogRecordPos{Fid: 1, Offset: 20})
	assert.Equal(t, 2, bt.Size())

	bt.Delete([]byte("aa"))
	assert.Equal(t, 1, bt.Size())
}
func TestBTree_Iterator(t *testing.T) {
	bt1 := index.NewBTree()

	// case 1: Btree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.NotNil(t, iter1)
	assert.Equal(t, false, iter1.Valid())

	// case 2: Btree 中有数据的情况
	bt1.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.Equal(t, []byte("aa"), iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	iter2.Close()

	// case 3: Btree 中有多个数据的情况
	bt1.Put([]byte("cc"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt1.Put([]byte("bb"), &data.LogRecordPos{Fid: 1, Offset: 30})
	iter3 := bt1.Iterator(false)
	assert.Equal(t, true, iter3.Valid())
	assert.Equal(t, []byte("aa"), iter3.Key())
	assert.NotNil(t, iter3.Value())
	iter3.Next()
	assert.Equal(t, []byte("bb"), iter3.Key())
	assert.NotNil(t, iter3.Value())
	iter3.Next()
	assert.Equal(t, []byte("cc"), iter3.Key())
	assert.NotNil(t, iter3.Value())
	iter3.Close()

	// case 4: reverse 遍历的情况
	iter4 := bt1.Iterator(true)
	assert.Equal(t, true, iter4.Valid())
	assert.Equal(t, []byte("cc"), iter4.Key())
	assert.NotNil(t, iter4.Value())
	iter4.Next()
	assert.Equal(t, []byte("bb"), iter4.Key())
	assert.NotNil(t, iter4.Value())
	iter4.Next()
	assert.Equal(t, []byte("aa"), iter4.Key())
	assert.NotNil(t, iter4.Value())
	iter4.Close()

	// case 6: Seek 的情况
	iter5 := bt1.Iterator(false)
	iter5.Seek([]byte("bb"))
	assert.Equal(t, []byte("bb"), iter5.Key())
	assert.NotNil(t, iter5.Value())
	iter5.Next()
	assert.Equal(t, []byte("cc"), iter5.Key())
	assert.NotNil(t, iter5.Value())
	iter5.Close()

	// case 7: 反向 seek 的情况
	iter6 := bt1.Iterator(true)
	iter6.Seek([]byte("bb"))
	assert.Equal(t, []byte("bb"), iter6.Key())
	assert.NotNil(t, iter6.Value())
	iter6.Next()
	assert.Equal(t, []byte("aa"), iter6.Key())
	assert.NotNil(t, iter6.Value())
	iter6.Close()
}
