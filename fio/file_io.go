package fio

import "os"

const DataFIlePerm = 0644

// FileIO 标准文件系统的 IO
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 创建一个新的 FileIO
func NewFileIOManager(filename string) (*FileIO, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFIlePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}

func (fio *FileIO) Write(buf []byte) (int, error) {
	return fio.fd.Write(buf)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
