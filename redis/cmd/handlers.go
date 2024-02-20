package main

import (
	"fairy-kvdb/utils"
	"github.com/tidwall/redcon"
)

// 处理 SET 命令
// Example: `SET key value`
func set(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}
	key, value := args[0], args[1]
	if err := client.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

// 处理 GET 命令
// Example: `GET key`
func get(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	key := args[0]
	value, err := client.db.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return value, nil
}

// 处理 HSET 命令
// Example: `HSET key field value`
func hset(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}
	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := client.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

// 处理 SADD 命令
// Example: `SADD key member`
func sadd(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	var ok = 0
	key, member := args[0], args[1]
	res, err := client.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

// 处理 LPUSH 命令
// Example: `LPUSH key value`
func lpush(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}
	key, value := args[0], args[1]
	listSize, err := client.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(listSize), nil
}

// 处理 ZADD 命令
// Example: `ZADD key score member`
func zadd(client *RedisClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}
	var ok = 0
	key, score, member := args[0], args[1], args[2]
	addSuccess, err := client.db.ZAdd(key, utils.DecodeFloat64(score), member)
	if err != nil {
		return nil, err
	}
	if addSuccess {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
