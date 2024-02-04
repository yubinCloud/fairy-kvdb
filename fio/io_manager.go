package fio

type IOManager interface {
	// Read 从指定位置读取指定长度的数据
	Read([]byte, int64) (int, error)

	// Write 向指定位置写入指定长度的数据
	Write([]byte) (int, error)

	// Sync 同步数据到磁盘
	Sync() error

	// Close 关闭文件
	Close() error
}
