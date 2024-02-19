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

func TestRedisDataStructure_ZSet(t *testing.T) {
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	rds, err := redis.NewRedisDataStructure(options)
	defer func() {
		_ = rds.Close()
		test.ClearDatabaseDir(options.DataDir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	// 测试 ZAdd
	const count = 10
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			score := float64(j * 5)
			ok, err := rds.ZAdd(key, score, member)
			assert.Nil(t, err)
			assert.True(t, ok)
		}
	}

	// 测试 ZScore
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+5; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			score, err := rds.ZScore(key, member)
			if j < i+1 {
				assert.Nil(t, err)
				assert.Equal(t, float64(j*5), score)
			} else {
				assert.True(t, errors.Is(err, fairydb.ErrorKeyNotFound))
				assert.Equal(t, float64(-1), score)
			}
		}
		for j := i + 5; j < i+10; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			score, err := rds.ZScore(key, member)
			assert.True(t, errors.Is(err, fairydb.ErrorKeyNotFound))
			assert.Equal(t, float64(-1), score)
		}
	}
}
