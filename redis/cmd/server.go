package main

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const redisAddr = "0.0.0.0:6380"

type RedisServer struct {
	dbs    map[int]*redis.DataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	rds, err := redis.NewRedisDataStructure(fairydb.DefaultOptions)
	if err != nil {
		panic(err)
	}
	// 初始化一个 Redis 服务端
	redisServer := &RedisServer{
		dbs: make(map[int]*redis.DataStructure),
	}
	redisServer.dbs[0] = rds
	redisServer.server = redcon.NewServer(redisAddr, execClientCommand, redisServer.accept, redisServer.close)
	// server 启动监听
	redisServer.listen()
}

func (rs *RedisServer) listen() {
	log.Println("FairyDB redis server running, ready to accept connections.")
	_ = rs.server.ListenAndServe()
}

func (rs *RedisServer) accept(conn redcon.Conn) bool {
	client := new(RedisClient)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	client.server = rs
	client.db = rs.dbs[0] // 这里可以扩展
	conn.SetContext(client)
	return true
}

func (rs *RedisServer) close(conn redcon.Conn, err error) {
	for _, db := range rs.dbs {
		_ = db.Close()
	}
	_ = rs.server.Close()
}
