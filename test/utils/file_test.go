package utils

import (
	fairy_kvdb "fairy-kvdb"
	"fairy-kvdb/test"
	"fairy-kvdb/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDirSize(t *testing.T) {
	dirPath := fairy_kvdb.DefaultOptions.DataDir
	options := fairy_kvdb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	db, err := fairy_kvdb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = db.Sync()
	assert.Nil(t, err)
	size, err := utils.DirSize(dirPath)
	assert.Nil(t, err)
	assert.Less(t, int64(0), size)

	err = db.Close()
	assert.Nil(t, err)
	test.ClearDatabaseDir(options.DataDir)
}
