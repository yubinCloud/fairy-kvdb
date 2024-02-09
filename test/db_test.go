package test

import (
	fairydb "fairy-kvdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_ListKeys(t *testing.T) {
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

	keys := db.ListKeys()
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, "age", string(keys[0]))
	assert.Equal(t, "name", string(keys[1]))

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Fold(t *testing.T) {
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
	err = db.Put([]byte("sex"), []byte("1"))
	assert.Nil(t, err)

	// case 1: count
	cnt := 0
	err = db.Fold(func(key, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		cnt++
		if string(key) == "name" {
			return false
		} else {
			return true
		}
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, cnt)

	// case 2: length sum
	lengthSum := 0
	err = db.Fold(func(key, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		lengthSum += len(value)
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, 11, lengthSum)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}
