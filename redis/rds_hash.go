package redis

import (
	"encoding/binary"
	"errors"
	fairydb "fairy-kvdb"
)

// hashInternalKey 在 Hash 数据结构中，用于存储数据部分的 key 的组成结构
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hik *hashInternalKey) encode() []byte {
	size := len(hik.key) + binary.MaxVarintLen64 + len(hik.field)
	buf := make([]byte, size)
	offset := copy(buf, hik.key)
	offset += binary.PutVarint(buf[offset:], hik.version)
	copy(buf[offset:], hik.field)
	return buf
}

func (rds *DataStructure) HSet(key, field, value []byte) (bool, error) {
	// 查找对应的元数据
	meta, err := rds.findMetadata(key, RdsHash)
	if err != nil {
		return false, err
	}
	// 构造 Hash 数据部分的 key
	iKey := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := iKey.encode()
	// 先查找这个数据部分的 key 是否存在
	exist := true
	if _, err := rds.db.Get(encKey); err != nil && errors.Is(err, fairydb.ErrorKeyNotFound) {
		exist = false
	}
	// 如果不存在，则更新元数据
	writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	if !exist {
		meta.sz++
		_ = writeBatch.Put(key, meta.encode())
	}
	_ = writeBatch.Put(encKey, value) // 更新数据部分
	if err = writeBatch.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructure) HGet(key, field []byte) ([]byte, error) {
	// 查找对应的元数据
	meta, err := rds.findMetadata(key, RdsHash)
	if err != nil {
		return nil, err
	}
	// 构造 Hash 数据部分的 key
	iKey := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := iKey.encode()
	// 查找数据部分
	return rds.db.Get(encKey)
}

func (rds *DataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RdsHash)
	if err != nil {
		return false, err
	}
	if meta.sz == 0 { // 表示这个 key 没找到
		return false, nil
	}
	iKey := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := iKey.encode()
	// 先查找这个数据部分的 key 是否存在
	exist := true
	if _, err := rds.db.Get(encKey); err != nil && errors.Is(err, fairydb.ErrorKeyNotFound) {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
		meta.sz--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}
