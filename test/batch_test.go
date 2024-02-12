package test

import (
	fairydb "fairy-kvdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	options1 := fairydb.DefaultOptions
	ClearDatabaseDir(options1.DataDir)
	db, err := fairydb.Open(options1)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// write batch 写入数据
	wb := db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	err = wb.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = wb.Put([]byte("age"), []byte("18"))
	assert.Nil(t, err)
	err = wb.Put([]byte("sex"), []byte("1"))
	assert.Nil(t, err)
	err = wb.Delete([]byte("age"))
	assert.Nil(t, err)

	// batch 还未 commit，因此不应该查出数据
	val1, err := db.Get([]byte("name"))
	assert.Equal(t, fairydb.ErrorKeyNotFound, err)
	assert.Nil(t, val1)

	// batch commit 后
	err = wb.Commit()
	assert.Nil(t, err)
	val2, err := db.Get([]byte("name"))
	assert.Nil(t, err)
	assert.Equal(t, "zhangSan", string(val2))
	val3, err := db.Get([]byte("age"))
	assert.Equal(t, fairydb.ErrorKeyNotFound, err)
	assert.Nil(t, val3)
	val4, err := db.Get([]byte("sex"))
	assert.Nil(t, err)
	assert.Equal(t, "1", string(val4))

	// 只含有一个 delete 操作的 batch
	wb2 := db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	err = wb2.Delete([]byte("sex"))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	// Reboot
	options2 := fairydb.DefaultOptions
	db, err = fairydb.Open(options2)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	val5, err := db.Get([]byte("name"))
	assert.Nil(t, err)
	assert.Equal(t, "zhangSan", string(val5))
	val6, err := db.Get([]byte("age"))
	assert.Equal(t, fairydb.ErrorKeyNotFound, err)
	assert.Nil(t, val6)

	err = db.Close()
	assert.Nil(t, err)
	ClearDatabaseDir(options2.DataDir)
}
