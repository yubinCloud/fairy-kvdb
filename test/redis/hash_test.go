package redis

import (
	"errors"
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestRedisDataStructure_Hash(t *testing.T) {
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	rds, err := redis.NewRedisDataStructure(options)
	defer func() {
		_ = rds.Close()
		test.ClearDatabaseDir(options.DataDir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	// 测试 HSet
	const count = 20
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			value := []byte("value-" + strconv.Itoa(j*10))
			ok, err := rds.HSet(key, field, value)
			assert.Nil(t, err)
			assert.True(t, ok)
		}
	}
	// 测试 HGet
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+2; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			if j == i+1 {
				value, err := rds.HGet(key, field)
				assert.True(t, errors.Is(err, fairydb.ErrorKeyNotFound))
				assert.Nil(t, value)
				continue
			}
			value, err := rds.HGet(key, field)
			assert.Nil(t, err)
			assert.Equal(t, "value-"+strconv.Itoa(j*10), string(value))
		}
	}
	// 测试 HDel
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			if j%2 == 0 {
				ok, err := rds.HDel(key, field)
				assert.Nil(t, err)
				assert.True(t, ok)
			}
		}
	}
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			value, err := rds.HGet(key, field)
			if j%2 != 0 {
				assert.Nil(t, err)
				assert.Equal(t, "value-"+strconv.Itoa(j*10), string(value))
			} else {
				assert.True(t, errors.Is(err, fairydb.ErrorKeyNotFound))
				assert.Nil(t, value)
			}
		}
	}
	// 测试 HSet 设置新值
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			if j%3 == 0 {
				value := []byte("value-" + strconv.Itoa(j*100))
				ok, err := rds.HSet(key, field, value)
				assert.Nil(t, err)
				if j%2 == 0 {
					assert.True(t, ok)
				} else {
					assert.False(t, ok)
				}
			}
		}
	}
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			field := []byte("field-" + strconv.Itoa(j))
			value, err := rds.HGet(key, field)
			if j%3 == 0 {
				assert.Nil(t, err)
				assert.Equal(t, "value-"+strconv.Itoa(j*100), string(value))
			}
		}
	}
}
