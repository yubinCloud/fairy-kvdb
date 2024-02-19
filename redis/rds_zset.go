package redis

import (
	"encoding/binary"
	"errors"
	fairydb "fairy-kvdb"
	"fairy-kvdb/utils"
)

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zik *zsetInternalKey) encode(withScore bool) []byte {
	var scoreBuf []byte
	memberSize := uint32(len(zik.member))
	bufLen := len(zik.key) + binary.MaxVarintLen64 + len(zik.member) + 4
	if withScore {
		scoreBuf = utils.EncodeFloat64(zik.score)
		bufLen += len(scoreBuf)
	}
	buf := make([]byte, bufLen)
	offset := 0
	offset += copy(buf[offset:], zik.key)
	offset += binary.PutVarint(buf[offset:], zik.version)
	offset += copy(buf[offset:], zik.member)
	binary.BigEndian.PutUint32(buf[offset:], memberSize)
	offset += 4
	if withScore {
		offset += copy(buf[offset:], scoreBuf)
	}
	return buf[:offset]
}

func (rds *DataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RdsZSet)
	if err != nil {
		return false, err
	}
	// 构造数据部分的 key
	iKey := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}
	// 检查 `key|version|member` 是否已经存在
	exist := true
	value, err := rds.db.Get(iKey.encode(false))
	if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
		return false, err
	}
	if errors.Is(err, fairydb.ErrorKeyNotFound) {
		exist = false
	}
	if exist {
		if score == utils.DecodeFloat64(value) {
			return false, nil
		}
	}
	// 更新元数据和数据
	writeBatch := rds.db.NewWriteBatch(fairydb.DefaultWriteBatchOptions)
	if !exist {
		meta.sz++
		_ = writeBatch.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.DecodeFloat64(value),
		}
		_ = writeBatch.Delete(oldKey.encode(true))
	}
	_ = writeBatch.Put(iKey.encode(false), utils.EncodeFloat64(score)) // `key|version|member` 的值是 score
	_ = writeBatch.Put(iKey.encode(true), nil)                         // `key|version|member|score` 的值是空
	if err = writeBatch.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, RdsZSet)
	if err != nil || meta.sz == 0 {
		return -1, err
	}
	iKey := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	value, err := rds.db.Get(iKey.encode(false))
	if err != nil {
		return -1, err
	}
	return utils.DecodeFloat64(value), nil
}
