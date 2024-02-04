package data

// LogRecordPos 数据内存索引，主要是描述数据再磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // file ID，表示将数据存储到了哪个文件中
	Offset int64  // offset，表示将数据存放到了数据文件的哪个位置
}
