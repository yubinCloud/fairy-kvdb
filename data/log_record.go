package data

import (
	"encoding/binary"
	"hash/crc32"
)

// 文件中一个 LogRecordHeader 的长度
// Crc  type  KeySize  ValueSize
//
//	4 + 1 + 10 +  5      +  5      = 25
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen64 + binary.MaxVarintLen32*2

// LogRecordPos 数据内存索引，主要是描述数据再磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // file ID，表示将数据存储到了哪个文件中
	Offset int64  // offset，表示将数据存放到了数据文件的哪个位置
}

// EncodeLogRecordPos 对 LogRecordPos 进行序列化
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[:4], pos.Fid)
	binary.BigEndian.PutUint64(buf[4:], uint64(pos.Offset))
	return buf
}

// DecodeLogRecordPos 对 LogRecordPos 进行反序列化
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	if len(buf) != 12 {
		return nil
	}
	return &LogRecordPos{
		Fid:    binary.BigEndian.Uint32(buf[:4]),
		Offset: int64(binary.BigEndian.Uint64(buf[4:])),
	}
}

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordBatchEnd
)

// LogRecord 写入到数据文件的数据记录
type LogRecord struct {
	Key   []byte // key
	Value []byte // value
	Type  LogRecordType
	Btsn  uint64 // BTSN，Batch Transaction Sequence Number，用于唯一标识一个 batch transaction
}

// LogRecordHeader LogRecord 的头部信息
type LogRecordHeader struct {
	Crc       uint32 // CRC 校验码
	RecType   LogRecordType
	Btsn      uint64
	KeySize   uint32
	ValueSize uint32
}

// EncodeLogRecord 对 LogRecord 进行序列化
// 返回字节数组以及长度
// +-----------------+-----------------+---------------+-----------------+-----------------+------------------+-----------------+
// |      Crc        |     RecType     |     BSTN      | KeySize         |    ValueSize    |     key          |  value		    |
// +-----------------+-----------------+---------------+-  --------------+-----------------+------------------+-----------------+
// | 4 bytes         | 1 byte          |变长，最长8bytes | 变长，最大5bytes | 变长，最大5bytes  | KeySize bytes    | ValueSize bytes |
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	// 第 5 个字节存储 type
	header[4] = byte(record.Type)
	var offset = 5
	// 5 字节后，存储 LSN
	btsn := record.Btsn
	offset += binary.PutUvarint(header[offset:], btsn)
	// 之后存储的是 key 和 value 的长度信息
	keySize := int64(len(record.Key))
	valueSize := int64(len(record.Value))
	offset += binary.PutVarint(header[offset:], keySize)
	offset += binary.PutVarint(header[offset:], valueSize)
	totalSize := int64(offset) + keySize + valueSize
	encBytes := make([]byte, totalSize)
	// 将 header 部分的内容拷贝到 encBytes 中
	copy(encBytes, header)
	// 将 key 和 value 拷贝到 encBytes 中
	copy(encBytes[offset:], record.Key)
	copy(encBytes[offset+len(record.Key):], record.Value)
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes, crc)
	return encBytes, totalSize
}

// DecodeLogRecordHeader 对 LogRecordHeader 进行反序列化
func DecodeLogRecordHeader(buf []byte) (record *LogRecordHeader, length int64) {
	if len(buf) <= 4 { // 如果连 Crc 都没有，则直接返回
		return nil, 0
	}
	header := &LogRecordHeader{
		Crc:     binary.LittleEndian.Uint32(buf[:4]),
		RecType: LogRecordType(buf[4]),
	}
	var offset = 5
	// 读取 BTSN
	btsn, n := binary.Uvarint(buf[offset:])
	offset += n
	// 读取 key 和 value 的长度
	keySize, n := binary.Varint(buf[offset:])
	offset += n
	valueSize, n := binary.Varint(buf[offset:])
	offset += n
	header.Btsn = btsn
	header.KeySize = uint32(keySize)
	header.ValueSize = uint32(valueSize)
	return header, int64(offset)
}

// ComputeCRC 计算 LogRecord 的 CRC 校验码
// 这里传入的 header 不包含前 4 个字节
func ComputeCRC(record *LogRecord, header []byte) uint32 {
	if record == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)
	return crc
}

const NoTxnBTSN = 0

type BatchTxnRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
