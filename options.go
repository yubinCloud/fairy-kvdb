package fairy_kvdb

import (
	"fairy-kvdb/index"
	"os"
	"path/filepath"
)

type Options struct {
	DataDir            string // 数据库数据目录
	MaxFileSize        int64  // 数据文件最大大小
	SyncEveryWrite     bool   // 是否每次写入都同步
	IndexType          int8   // 索引类型
	BPlusTreeIndexOpts *index.BPlusTreeIndexOptions
}

type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认为 false
	Reverse bool
}

var DefaultOptions = Options{
	DataDir:            filepath.Join(os.TempDir(), "fairy-kvdb"),
	MaxFileSize:        256 * 1024 * 1024, // 256 MB
	SyncEveryWrite:     false,
	IndexType:          int8(index.BTreeIndexer),
	BPlusTreeIndexOpts: nil,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量写入的配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum int

	// 提交时是否 sync 持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
