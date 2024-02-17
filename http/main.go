package main

import (
	fairydb "fairy-kvdb"
	"fairy-kvdb/http/route"
	"fmt"
	"github.com/gin-gonic/gin"
)

var db *fairydb.DB

func init() {
	var err error
	// 初始化 DB 实例
	options := fairydb.DefaultOptions
	db, err = fairydb.Open(options)
	if err != nil {
		panic(fmt.Sprintf("Failed to open database: %v", err))
	}
}

func main() {
	// 设置路由
	r := gin.Default()
	// basic controller
	basicRoute := r.Group("/basic")
	route.RegisterBasicRoute(basicRoute, db)
	// 启动 HTTP 服务
	err := r.Run(":7250")
	if err != nil {
		panic(fmt.Sprintf("Failed to start HTTP server: %v", err))
	}
}
