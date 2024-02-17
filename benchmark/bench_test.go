package benchmark

import (
	"errors"
	fairydb "fairy-kvdb"
	"fairy-kvdb/test"
	"fairy-kvdb/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var db *fairydb.DB

func init() {
	// 初始化用于基准测试的存储引擎
	options := fairydb.DefaultOptions
	test.ClearDatabaseDir(options.DataDir)

	var err error
	db, err = fairydb.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()   // 重置计时器
	b.ReportAllocs() // 开启内存分配统计

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		err := db.Put([]byte(key), utils.RandomTestValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i*2)
		err := db.Put([]byte(key), utils.RandomTestValue(1024))
		assert.Nil(b, err)
	}
	// 测试 GET
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", int64(i*3)%10000)
		_, err := db.Get([]byte(key))
		if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	// 先写入一些数据
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i*2)
		err := db.Put([]byte(key), utils.RandomTestValue(1024))
		assert.Nil(b, err)
	}
	// 测试 DELETE
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", (i*3)%10000)
		err := db.Delete([]byte(key))
		if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
			b.Fatal(err)
		}
	}
}
