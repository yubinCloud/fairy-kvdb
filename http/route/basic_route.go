package route

import (
	"errors"
	fairydb "fairy-kvdb"
	"fairy-kvdb/http/dto"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

var db *fairydb.DB

// RegisterBasicRoute 注册 `/basic` 路由
func RegisterBasicRoute(basicRoute *gin.RouterGroup, dbArg *fairydb.DB) {
	db = dbArg

	basicRoute.PUT("/put", BasicPut)
	basicRoute.GET("/get", BasicGet)
	basicRoute.DELETE("/delete", BasicDelete)
	basicRoute.GET("/keys", BasicListKeys)
	basicRoute.GET("/stat", BasicStat)
}

// BasicPut 通过 PUT 方法向数据库中写入数据
func BasicPut(c *gin.Context) {
	body := make(map[string]string)
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for key, value := range body {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			log.Printf("Failed to put data into database: %v", err)
			return
		}
	}
}

// BasicGet 通过 GET 方法从数据库中读取数据
func BasicGet(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	value, err := db.Get([]byte(key))
	if err != nil && !errors.Is(err, fairydb.ErrorKeyNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		log.Printf("Failed to get data from database: %v", err)
		return
	}
	exists := true
	if errors.Is(err, fairydb.ErrorKeyNotFound) {
		exists = false
	}
	c.JSON(http.StatusOK, dto.BasicGetResponse{
		Exists: exists,
		Value:  string(value),
	})
	return
}

func BasicDelete(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	err := db.Delete([]byte(key))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		log.Printf("Failed to delete data from database: %v", err)
		return
	}
	return
}

func BasicListKeys(c *gin.Context) {
	keys := db.ListKeys()
	results := make([]string, len(keys))
	for i, key := range keys {
		results[i] = string(key)
	}
	c.JSON(http.StatusOK, results)
	return
}

func BasicStat(c *gin.Context) {
	stats := db.Stat()
	c.JSON(http.StatusOK, stats)
	return
}
