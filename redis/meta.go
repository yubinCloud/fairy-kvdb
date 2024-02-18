package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraMetaDataSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

// 某些 Redis 数据结构所需要的元数据
type metaData struct {
	valueType RdsValueType // 数据类型
	expire    int64        // 过期时间
	version   int64        // 版本号
	sz        uint32       // 数据大小
	head      uint64       // List 专有数据
	tail      uint64       // List 专有数据
}

func (md *metaData) encode() []byte {
	// make buf
	size := maxMetaDataSize
	if md.valueType == RdsList {
		size += extraMetaDataSize
	}
	buf := make([]byte, size)
	// encode metadata
	buf[0] = md.valueType
	offset := 1
	offset += binary.PutVarint(buf[offset:], md.expire)
	offset += binary.PutVarint(buf[offset:], md.version)
	offset += binary.PutUvarint(buf[offset:], uint64(md.sz))
	if md.valueType == RdsList {
		offset += binary.PutUvarint(buf[offset:], md.head)
		offset += binary.PutUvarint(buf[offset:], md.tail)
	}
	return buf[:offset]
}

func (md *metaData) decode(buf []byte) {
	md.valueType = buf[0]

	offset := 1
	var n int
	md.expire, n = binary.Varint(buf[offset:])
	offset += n
	md.version, n = binary.Varint(buf[offset:])
	offset += n
	sz, n := binary.Uvarint(buf[offset:])
	md.sz = uint32(sz)
	offset += n
	if md.valueType == RdsList {
		md.head, n = binary.Uvarint(buf[offset:])
		offset += n
		md.tail, n = binary.Uvarint(buf[offset:])
		offset += n
	}
}
