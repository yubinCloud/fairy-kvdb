package main

import (
	"errors"
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"github.com/tidwall/redcon"
	"strings"
)

type cmdHandler func(client *RedisClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"SET":   set,
	"GET":   get,
	"HSET":  hset,
	"SADD":  sadd,
	"LPUSH": lpush,
	"ZADD":  zadd,
}

type RedisClient struct {
	server *RedisServer
	db     *redis.DataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToUpper(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + command + "'") // 对于不支持的命令，直接向 client 报错即可
		return
	}

	client, _ := conn.Context().(*RedisClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, fairydb.ErrorKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}
