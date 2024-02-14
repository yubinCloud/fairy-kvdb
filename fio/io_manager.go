package fio

type FileIOType = byte

const (
	StandardFIO FileIOType = iota // 标准文件 IO
	MemoryMapIO                   // 内存文件映射 IO
)

type IOManager interface {
	// Read 从指定位置读取指定长度的数据
	Read([]byte, int64) (int, error)

	// Write 向指定位置写入指定长度的数据
	Write([]byte) (int, error)

	// Sync 同步数据到磁盘
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 创建一个 IOManager，目前只支持 FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMapIO:
		return NewMMapIOManager(fileName)
	default:
		panic("unknown io type")
	}
}
