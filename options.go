package fairy_kvdb

import (
	"fairy-kvdb/index"
	"os"
	"path/filepath"
)

type Options struct {
	DataDir        string // 数据库数据目录
	MaxFileSize    int64  // 数据文件最大大小
	SyncEveryWrite bool   // 是否每次写入都同步
	IndexType      int8   // 索引类型
}

type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认为 false
	Reverse bool
}

var DefaultOptions = Options{
	DataDir:        filepath.Join(os.TempDir(), "fairy-kvdb"),
	MaxFileSize:    256 * 1024 * 1024, // 256 MB
	SyncEveryWrite: false,
	IndexType:      int8(index.BTreeIndex),
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
