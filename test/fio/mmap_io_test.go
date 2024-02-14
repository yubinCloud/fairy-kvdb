package fio

import (
	fairy_kvdb "fairy-kvdb"
	"fairy-kvdb/fio"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestMMapIO_Read(t *testing.T) {
	path := filepath.Join(fairy_kvdb.DefaultOptions.DataDir, "test_mmap_io.test")
	_ = os.Remove(path)
	defer func() {
		_ = os.Remove(path)
	}()
	mmapIO, err := fio.NewMMapIOManager(path)
	assert.Nil(t, err)

	// 当文件为空时
	buf := make([]byte, 10)
	n, err := mmapIO.Read(buf, 0)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// 使用标准文件系统的 IO 写入一部分数据
	fileIO, err := fio.NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("hello"))
	assert.Nil(t, err)
	err = fileIO.Sync()
	assert.Nil(t, err)
	err = fileIO.Close()
	assert.Nil(t, err)

	// 使用 mmap 读取
	buf = make([]byte, 10)
	mmapIO, err = fio.NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(5), size)
	n, err = mmapIO.Read(buf, 0)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf[:n]))
}
