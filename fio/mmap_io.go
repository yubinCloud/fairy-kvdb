package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMapIO 内存文件映射实现的 IO
type MMapIO struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(filename string) (*MMapIO, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, DataFIlePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMapIO{readerAt: readerAt}, nil
}

func (mmap *MMapIO) Read(buf []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(buf, offset)
}

func (mmap *MMapIO) Write(buf []byte) (int, error) {
	return 0, nil
}

func (mmap *MMapIO) Sync() error {
	return nil
}

func (mmap *MMapIO) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}

func (mmap *MMapIO) Close() error {
	return mmap.readerAt.Close()
}
