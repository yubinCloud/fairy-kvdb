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

var DefaultOptions = Options{
	DataDir:        filepath.Join(os.TempDir(), "fairy-kvdb"),
	MaxFileSize:    256 * 1024 * 1024, // 256 MB
	SyncEveryWrite: false,
	IndexType:      int8(index.BTreeIndex),
}
