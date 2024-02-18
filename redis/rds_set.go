package redis

import (
	"encoding/binary"
	"errors"
	fairydb "fairy-kvdb"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sik *setInternalKey) encode() []byte {
	buf := make([]byte, len(sik.key)+binary.MaxVarintLen64+len(sik.member)+4)
	// key
	var offset = 0
	offset += copy(buf[:len(sik.key)], sik.key)
	// version
	offset += binary.PutVarint(buf[offset:], sik.version)
	// member
	offset += copy(buf[offset:], sik.member)
	// memberSize
	memberSize := len(sik.member)
	binary.BigEndian.PutUint32(buf[offset:], uint32(memberSize))
	offset += 4
	return buf[:offset]
}

func (rds *DataStructure) SAdd(key, member []byte) (bool, error) {
	// 查找对应的元数据
	meta, err := rds.findMetadata(key, RdsSet)
	if err != nil {
		return false, err
	}
	// 构造一个数据部分的 key
	iKey := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := iKey.encode()
	// 查找这个数据部分的 key 是否存在
	addSuccess := false
	if _, err = rds.db.Get(encKey); errors.Is(err, fairydb.ErrorKeyNotFound) {
		// 不存在的话则 add
		writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
		meta.sz++
		_ = writeBatch.Put(key, meta.encode())
		_ = writeBatch.Put(encKey, nil)
		if err = writeBatch.Commit(); err != nil {
			return false, err
		}
		addSuccess = true
	}
	return addSuccess, nil
}

func (rds *DataStructure) SIsMember(key, member []byte) (bool, error) {
	// 查找对应的元数据
	meta, err := rds.findMetadata(key, RdsSet)
	if err != nil {
		return false, err
	}
	// 构造一个数据部分的 key
	iKey := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := iKey.encode()
	// 查找这个数据部分的 key 是否存在
	_, err = rds.db.Get(encKey)
	if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
		return false, err
	}
	if errors.Is(err, fairydb.ErrorKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *DataStructure) SRem(key, member []byte) (bool, error) {
	// 查找对应的元数据
	meta, err := rds.findMetadata(key, RdsSet)
	if err != nil {
		return false, err
	}
	// 构造一个数据部分的 key
	iKey := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := iKey.encode()
	// 查找这个数据部分的 key 是否存在
	_, err = rds.db.Get(encKey)
	if errors.Is(err, fairydb.ErrorKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// 删除数据部分的 key
	writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	meta.sz--
	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Delete(encKey)
	if err = writeBatch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
