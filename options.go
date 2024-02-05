package fairy_kvdb

type Options struct {
	DataDir        string // 数据库数据目录
	MaxFileSize    int64  // 数据文件最大大小
	SyncEveryWrite bool   // 是否每次写入都同步
}
