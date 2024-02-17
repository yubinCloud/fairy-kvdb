package redis

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestRedisDataStructure_String(t *testing.T) {
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	rds, err := redis.NewRedisDataStructure(options)
	defer func() {
		err := rds.Close()
		assert.Nil(t, err)
		test.ClearDatabaseDir(options.DataDir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	count := 100

	// 测试 Set 和 Get
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			err = rds.Set(key, value, 0)
			assert.Nil(t, err)
		}
	}
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		if i%2 == 0 {
			value, err := rds.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, "value-"+strconv.Itoa(i), string(value))
		} else {
			value, err := rds.Get(key)
			assert.Nil(t, err)
			assert.Nil(t, value)
		}
	}
	// 测试 expire
	err = rds.Set([]byte("name"), []byte("zhangSan"), 1)
	time.Sleep(2 * time.Second)
	value, err := rds.Get([]byte("name"))
	assert.Nil(t, value)
	assert.Nil(t, err)
	// 测试 Delete
	for i := 0; i < count; i++ {
		if i%4 == 0 {
			key := []byte("key-" + strconv.Itoa(i))
			err = rds.Del(key)
			assert.Nil(t, err)
		}
	}
	for i := 0; i < count; i++ {
		if i%2 != 0 || i%4 == 0 {
			key := []byte("key-" + strconv.Itoa(i))
			value, err := rds.Get(key)
			assert.Nil(t, value)
			assert.Nil(t, err)
		} else {
			key := []byte("key-" + strconv.Itoa(i))
			value, err := rds.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, "value-"+strconv.Itoa(i), string(value))
		}
	}
}
