package test

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/index"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestDB_BPlusTreeIndexDB(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	options.IndexType = int8(index.BPlusTreeIndexer)
	options.BPlusTreeIndexOpts = &index.BPlusTreeIndexOptions{
		BboltOptions: nil,
		DataDir:      filepath.Join(fairydb.DefaultOptions.DataDir, "bptree"),
	}
	defer ClearDatabaseDir(options.DataDir)
	// 启动数据库
	db, err := fairydb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 插入数据
	const count = 10
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err = db.Put(key, []byte(fmt.Sprintf("value%d", i)))
		assert.Nil(t, err)
	}
	// case: Get
	for i := 10; i < count+5; i++ {
		value, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if i < count {
			assert.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
		} else {
			assert.NotNil(t, err)
		}
	}
	// case: Delete
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			err = db.Delete([]byte(fmt.Sprintf("key%d", i)))
			assert.Nil(t, err)
		}
	}
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			value, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
			assert.NotNil(t, err)
			assert.Nil(t, value)
		} else {
			value, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
			assert.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
		}
	}
	// case: Put again
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err = db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i*10)))
			assert.Nil(t, err)
		}
	}
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			value, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
			assert.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("value%d", i*10)), value)
		}
	}
	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	defer ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	db2, err := fairydb.Open(options) // 重复的 open
	assert.NotNil(t, err)
	assert.Nil(t, db2)
	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Stat(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = db.Put([]byte("age"), []byte("18"))
	assert.Nil(t, err)
	err = db.Put([]byte("sex"), []byte("1"))
	assert.Nil(t, err)
	err = db.Delete([]byte("name"))
	assert.Nil(t, err)

	stat := db.Stat()
	assert.Equal(t, uint(2), stat.KeyNum)
	assert.Less(t, uint64(0), stat.ReclaimableSize)

	err = db.Close()
	ClearDatabaseDir(options.DataDir)
	assert.Nil(t, err)

	db2, err := fairydb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	err = db2.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = db2.Put([]byte("age"), []byte("18"))
	assert.Nil(t, err)
	err = db2.Put([]byte("sex"), []byte("1"))
	assert.Nil(t, err)
	err = db2.Put([]byte("name"), []byte("liSi"))
	assert.Nil(t, err)

	stat = db2.Stat()
	assert.Equal(t, uint(3), stat.KeyNum)
	assert.Less(t, uint64(0), stat.ReclaimableSize)

	err = db2.Close()
	assert.Nil(t, err)
	ClearDatabaseDir(options.DataDir)
}

func TestDB_Backup(t *testing.T) {
	options := fairydb.DefaultOptions
	ClearDatabaseDir(options.DataDir)
	db, err := fairydb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("name"), []byte("zhangSan"))
	assert.Nil(t, err)
	err = db.Put([]byte("age"), []byte("18"))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)

	backupDir := filepath.Join(os.TempDir(), "fairy-kvdb-bk")
	err = db.CopyBackup(backupDir)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
	ClearDatabaseDir(options.DataDir)

	options.DataDir = backupDir
	backupDB, err := fairydb.Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, backupDB)
	value1, err := backupDB.Get([]byte("name"))
	assert.Nil(t, err)
	assert.Equal(t, "zhangSan", string(value1))
	value2, err := backupDB.Get([]byte("age"))
	assert.Nil(t, err)
	assert.Equal(t, "18", string(value2))

	err = backupDB.Close()
	assert.Nil(t, err)
	time.Sleep(time.Second * 2)
	ClearDatabaseDir(backupDir)
}
