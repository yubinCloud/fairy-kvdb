package data

import "fairy-kvdb/fio"

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId      uint32        // 文件 ID
	WriteOffset int64         // 写入位置
	IoManger    fio.IOManager // IO 管理器
}

// OpenDataFile 打开一个新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Read() error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(p []byte) error {
	return nil
}
