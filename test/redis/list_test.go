package redis

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestRedisDataStructure_List(t *testing.T) {
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	rds, err := redis.NewRedisDataStructure(options)
	defer func() {
		_ = rds.Close()
		test.ClearDatabaseDir(options.DataDir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	// 测试 LPush 和 RPush
	const count = 8
	for i := 1; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j <= i*10; j++ {
			element := []byte(strconv.Itoa(j))
			if j%2 == 0 {
				sz, err := rds.LPush(key, element)
				assert.Nil(t, err)
				assert.Equal(t, uint32(j+1), sz)
			} else {
				sz, err := rds.RPush(key, element)
				assert.Nil(t, err)
				assert.Equal(t, uint32(j+1), sz)
			}
		}
	}

	// 测试 LPop 和 RPop
	for i := 1; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := i * 10; j >= 0; j-- {
			element := strconv.Itoa(j)
			if j%2 == 0 {
				e, err := rds.LPop(key)
				assert.Nil(t, err)
				assert.Equal(t, element, string(e))
			} else {
				e, err := rds.RPop(key)
				assert.Nil(t, err)
				assert.Equal(t, element, string(e))
			}
		}
		e, err := rds.LPop(key)
		assert.Nil(t, err)
		assert.Nil(t, e)
		e, err = rds.RPop(key)
		assert.Nil(t, err)
		assert.Nil(t, e)
	}
	absentKey := []byte("key-" + strconv.Itoa(0))
	element, err := rds.LPop(absentKey)
	assert.Nil(t, err)
	assert.Nil(t, element)
	element, err = rds.RPop(absentKey)
	assert.Nil(t, err)
	assert.Nil(t, element)
}
