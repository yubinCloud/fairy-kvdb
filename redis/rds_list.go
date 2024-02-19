package redis

import (
	"encoding/binary"
	fairydb "fairy-kvdb"
)

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lik *listInternalKey) encode() []byte {
	buf := make([]byte, len(lik.key)+binary.MaxVarintLen64+8)
	offset := 0
	offset += copy(buf[offset:], lik.key)
	offset += binary.PutVarint(buf[offset:], lik.version)
	binary.BigEndian.PutUint64(buf[offset:], lik.index)
	offset += 8
	return buf[:offset]
}

func (rds *DataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, true)
}

func (rds *DataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, false)
}

func (rds *DataStructure) LPop(key []byte) ([]byte, error) {
	return rds.pop(key, true)
}

func (rds *DataStructure) RPop(key []byte) ([]byte, error) {
	return rds.pop(key, false)
}

func (rds *DataStructure) push(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RdsList)
	if err != nil {
		return 0, err
	}
	// 根据 push 的方向，计算出 index
	var index uint64
	if isLeft {
		index = meta.head - 1
	} else {
		index = meta.tail
	}
	// 构造一个数据部分的 key
	iKey := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   index,
	}
	// 更新元数据和数据部分
	writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	meta.sz++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Put(iKey.encode(), element)
	if err = writeBatch.Commit(); err != nil {
		return 0, err
	}
	return meta.sz, nil
}

func (rds *DataStructure) pop(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RdsList)
	if err != nil {
		return nil, err
	}
	if meta.sz == 0 {
		return nil, nil
	}
	// 根据 pop 的方向，计算出 index
	var index uint64
	if isLeft {
		index = meta.head
	} else {
		index = meta.tail - 1
	}
	// 构造一个数据部分的 key
	iKey := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   index,
	}
	// 查找数据部分的 key
	element, err := rds.db.Get(iKey.encode())
	if err != nil {
		return nil, err
	}
	// 更新元数据和数据部分
	writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	meta.sz--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Delete(iKey.encode())
	if err = writeBatch.Commit(); err != nil {
		return nil, err
	}
	return element, nil
}
