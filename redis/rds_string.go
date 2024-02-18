package redis

import (
	"encoding/binary"
	"errors"
	fairydb "fairy-kvdb"
	"time"
)

// ====================== String 数据结构 ========================

func (rds *DataStructure) Set(key, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}
	// 编码 value：type + expire + payload
	encBuf := make([]byte, 1+binary.MaxVarintLen64+len(value))
	encBuf[0] = RdsString
	var offset = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	offset += binary.PutVarint(encBuf[offset:], expire)
	copy(encBuf[offset:], value)
	return rds.db.Put(key, encBuf[:offset+len(value)])
}

func (rds *DataStructure) Get(key []byte) ([]byte, error) {
	encBuf, err := rds.db.Get(key)
	if err != nil {
		if errors.Is(err, fairydb.ErrorKeyNotFound) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	// 解码 value
	if encBuf[0] != RdsString {
		return nil, ErrorWrongTypeOperation
	}
	expire, n := binary.Varint(encBuf[1:])
	if expire != 0 && time.Now().UnixNano() > expire {
		return nil, nil
	}
	return encBuf[n+1:], nil
}
