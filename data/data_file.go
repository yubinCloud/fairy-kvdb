package data

import (
	"fairy-kvdb/fio"
	"fmt"
	"io"
	"path/filepath"
)

const NameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件 ID
	WriteOffset int64         // 写入位置
	IoManger    fio.IOManager // IO 管理器
}

// OpenDataFile 打开一个新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := fmt.Sprintf("%09d", fileId) + NameSuffix
	path := filepath.Join(dirPath, fileName)
	// 创建一个新的 IOManager
	ioManager, err := fio.NewIOManager(path)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IoManger:    ioManager,
	}, nil
}

// ReadLogRecord 从指定位置读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (record *LogRecord, recordSize int64, err error) {
	// 获取文件大小
	fileSize, err := df.IoManger.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果获取的 header 最大大小超过了文件的长度，那么就只需要读取到文件末尾即可
	var headerReadSize int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerReadSize = fileSize - offset
	}
	// 读取 LogRecordHeader
	headerBuf, err := df.readNBytes(headerReadSize, offset)
	if err != nil {
		return nil, 0, err
	}
	// 解码 LogRecordHeader
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF // 表示已经读取完了，所以返回 EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF // 表示已经读取完了，所以返回 EOF
	}
	// 取出 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize = headerSize + keySize + valueSize
	record = &LogRecord{Type: header.recType}
	// 读取用户实际存储的 kv
	if keySize > 0 || valueSize > 0 {
		recordBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解码 LogRecord
		record.Key = recordBuf[:keySize]
		record.Value = recordBuf[keySize:]
	}
	// 校验 CRC
	crc := computeCRC(record, headerBuf[:headerSize])
	if crc != header.crc {
		return nil, 0, ErrorInvalidCRC
	}
	return record, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManger.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Read() error {
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManger.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManger.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IoManger.Read(buf, offset)
	return buf, err
}
