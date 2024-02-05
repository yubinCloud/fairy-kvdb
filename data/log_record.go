package data

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

// EncodeLogRecord 对 LogRecord 进行序列化
// 返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
