package redis

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestRedisDataStructure_Set(t *testing.T) {
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	rds, err := redis.NewRedisDataStructure(options)
	defer func() {
		_ = rds.Close()
		test.ClearDatabaseDir(options.DataDir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	// 测试 SAdd
	const count = 20
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			ok, err := rds.SAdd(key, member)
			assert.Nil(t, err)
			assert.True(t, ok)
		}
	}

	// 测试 SMember
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+2; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			if j == i+1 {
				ok, err := rds.SIsMember(key, member)
				assert.Nil(t, err)
				assert.False(t, ok)
				continue
			}
			ok, err := rds.SIsMember(key, member)
			assert.Nil(t, err)
			assert.True(t, ok)
		}
	}

	// 测试 SRem
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			if j%2 == 0 {
				ok, err := rds.SRem(key, member)
				assert.Nil(t, err)
				assert.True(t, ok)
			}
		}
	}
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			if j%2 == 0 {
				ok, err := rds.SIsMember(key, member)
				assert.Nil(t, err)
				assert.False(t, ok)
			} else {
				ok, err := rds.SIsMember(key, member)
				assert.Nil(t, err)
				assert.True(t, ok)
			}
		}
	}

	// 测试多次重复 SAdd
	for i := 0; i < count; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		for j := 0; j < i+1; j++ {
			member := []byte("member-" + strconv.Itoa(j))
			ok, err := rds.SAdd(key, member)
			assert.Nil(t, err)
			if j%2 == 0 {
				assert.True(t, ok)
			} else {
				assert.False(t, ok)
			}
		}
	}
}
