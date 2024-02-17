package redis

import (
	fairydb "fairy-kvdb"
)

type RdsValueType = byte

const (
	RdsString RdsValueType = iota
	RdsHash
	RdsSet
	RdsList
	RdsZSet
)

// DataStructure Redis 的数据结构
type DataStructure struct {
	db *fairydb.DB
}

// NewRedisDataStructure 创建一个新的 DataStructure 实例
func NewRedisDataStructure(options fairydb.Options) (*DataStructure, error) {
	db, err := fairydb.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataStructure{db: db}, nil
}

func (rds *DataStructure) Close() error {
	return rds.db.Close()
}
