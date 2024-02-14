package data

import (
	"encoding/binary"
	"fairy-kvdb/fio"
	"fmt"
	"io"
	"path/filepath"
)

const (
	NameSuffix            = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	BtsnFileName          = "btsn"
	BtsnFileKey           = "btsn"
)

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件 ID
	WriteOffset int64         // 写入位置
	IoManger    fio.IOManager // IO 管理器
}

// GetDataFilePath 根据数据目录路径和 file ID 获取数据文件路径
func GetDataFilePath(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+NameSuffix)
}

// OpenDataFile 打开一个新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	path := GetDataFilePath(dirPath, fileId)
	return newDataFile(path, fileId)
}

func newDataFile(filePath string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(filePath)
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
	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF // 表示已经读取完了，所以返回 EOF
	}
	// 取出 key 和 value 的长度
	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	recordSize = headerSize + keySize + valueSize
	// 初始化 LogRecord，并根据 header 填充 record
	record = &LogRecord{
		Type: header.RecType,
		Btsn: header.Btsn,
	}
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
	crc := ComputeCRC(record, headerBuf[4:headerSize])
	if crc != header.Crc {
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

// OpenHintFile 打开一个新的 hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, HintFileName)
	return newDataFile(filePath, 0)
}

// OpenMergeFinishedFile 打开（或创建）一个新的 merge 完成标志文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(filePath, 0)
}

// WriteHintRecord 写入 hint 索引记录
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// OpenBtsnFile 存储 batch transaction 序列号的文件，用于启动数据库时，数据库能够知道当前的 batch transaction 序列号，从而继续分配
// 对于 DB 启动时不需要遍历数据日志文件的索引类型，需要借用此文件来存储 batch transaction 序列号，比如 BPlusTreeIndex
func OpenBtsnFile(dirPath string) (*DataFile, error) {
	filePath := filepath.Join(dirPath, BtsnFileName)
	return newDataFile(filePath, 0)
}

func (df *DataFile) WriteBtsnRecord(btsn uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, btsn)
	record := &LogRecord{
		Key:   []byte(BtsnFileKey),
		Value: buf,
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) ReadBtsnRecord() (uint64, error) {
	record, _, err := df.ReadLogRecord(0)
	if err != nil {
		return 1, err
	}
	return binary.BigEndian.Uint64(record.Value), nil
}
