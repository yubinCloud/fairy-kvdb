package test

import (
	fairydb "fairy-kvdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iter := db.NewIterator(&fairydb.DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, false, iter.Valid())
	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Iterator_OneRecord(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)

	iter := db.NewIterator(&fairydb.DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, "name", string(iter.Key()))
	assert.Equal(t, "zhangSan", string(iter.Value()))

	iter.Next()
	assert.Equal(t, false, iter.Valid())

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Iterator_MultiRecords(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = db.Put([]byte("age"), []byte("18"))
	assert.Nil(t, err)

	// 测试正向迭代
	iter := db.NewIterator(&fairydb.DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, "age", string(iter.Key()))
	assert.Equal(t, "18", string(iter.Value()))
	iter.Next()
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, "name", string(iter.Key()))
	assert.Equal(t, "zhangSan", string(iter.Value()))
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()

	// 测试反向迭代
	iterOptions2 := fairydb.DefaultIteratorOptions
	iterOptions2.Reverse = true
	iter2 := db.NewIterator(&iterOptions2)
	assert.NotNil(t, iter2)
	iter2.Rewind()
	assert.Equal(t, true, iter2.Valid())
	assert.Equal(t, "name", string(iter2.Key()))
	assert.Equal(t, "zhangSan", string(iter2.Value()))
	iter2.Next()
	assert.Equal(t, true, iter2.Valid())
	assert.Equal(t, "age", string(iter2.Key()))
	assert.Equal(t, "18", string(iter2.Value()))
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	iter2.Close()

	// 测试指定 prefix 时的正向迭代
	iterOptions3 := fairydb.DefaultIteratorOptions
	iterOptions3.Prefix = []byte("n")
	iter3 := db.NewIterator(&iterOptions3)
	assert.NotNil(t, iter3)
	iter3.Rewind()
	assert.Equal(t, true, iter3.Valid())
	assert.Equal(t, "name", string(iter3.Key()))
	assert.Equal(t, "zhangSan", string(iter3.Value()))
	iter3.Next()
	assert.Equal(t, false, iter3.Valid())
	iter3.Close()

	// 测试 seek
	err = db.Put([]byte("sex"), []byte("1"))
	assert.Nil(t, err)
	iterOptions4 := fairydb.DefaultIteratorOptions
	iter4 := db.NewIterator(&iterOptions4)
	iter4.Seek([]byte("n"))
	assert.Equal(t, true, iter4.Valid())
	assert.Equal(t, "name", string(iter4.Key()))
	assert.Equal(t, "zhangSan", string(iter4.Value()))
	iter4.Next()
	assert.Equal(t, true, iter4.Valid())
	assert.Equal(t, "sex", string(iter4.Key()))
	assert.Equal(t, "1", string(iter4.Value()))

	err = db.Close()
	assert.Nil(t, err)
}
