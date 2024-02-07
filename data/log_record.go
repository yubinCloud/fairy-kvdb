package data

import "encoding/binary"

// 文件中一个 LogRecordHeader 的长度
// crc  type  keySize  valueSize
//
//	4 + 1 +    5      +  5      = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecordPos 数据内存索引，主要是描述数据再磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // file ID，表示将数据存储到了哪个文件中
	Offset int64  // offset，表示将数据存放到了数据文件的哪个位置
}

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// LogRecord 写入到数据文件的数据记录
type LogRecord struct {
	Key   []byte // key
	Value []byte // value
	Type  LogRecordType
}

// LogRecordHeader LogRecord 的头部信息
type LogRecordHeader struct {
	crc       uint32 // CRC 校验码
	recType   LogRecordType
	keySize   uint32
	valueSize uint32
}

// EncodeLogRecord 对 LogRecord 进行序列化
// 返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

// DecodeLogRecordHeader 对 LogRecordHeader 进行反序列化
func DecodeLogRecordHeader(buf []byte) (record *LogRecordHeader, length int64) {
	return nil, 0
}

func computeCRC(record *LogRecord, header []byte) uint32 {
	return 0
}
