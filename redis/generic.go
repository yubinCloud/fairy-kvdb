package redis

import (
	"errors"
	fairydb "fairy-kvdb"
	"time"
)

func (rds *DataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *DataStructure) Type(key []byte) (RdsValueType, error) {
	encBuf, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encBuf) == 0 {
		return 0, errors.New("value is null")
	}
	return encBuf[0], nil
}

func (rds *DataStructure) Keys() [][]byte {
	return rds.db.ListKeys()
}

// 查找一个 key 的 metadata
// 如果没找到这个 key，则返回一个新的 metadata
func (rds *DataStructure) findMetadata(key []byte, valueType RdsValueType) (*metaData, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
		return nil, err
	}
	var meta = &metaData{}
	var exist = true
	if errors.Is(err, fairydb.ErrorKeyNotFound) {
		exist = false
	} else {
		meta.decode(metaBuf)
		// 判断 value 类型
		if meta.valueType != valueType {
			return nil, ErrorWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire < time.Now().UnixNano() {
			exist = false
		}
	}
	// 如果 meta 不存在，则直接返回
	if !exist {
		meta.valueType = valueType
		meta.expire = 0
		meta.version = time.Now().UnixNano()
		meta.sz = 0
		if meta.valueType == RdsList {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
