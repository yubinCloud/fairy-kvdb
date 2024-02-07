package fio

import (
	"fairy-kvdb/fio"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	fileIo, err := fio.NewFileIOManager(filepath.Join(test.TempFilePath))
	defer test.DestroyFile(fileIo)
	assert.Nil(t, err)
	assert.NotNil(t, fileIo)
}

func TestFileIO_Write(t *testing.T) {
	fileIo, err := fio.NewFileIOManager(filepath.Join(test.TempFilePath))
	defer test.DestroyFile(fileIo)
	assert.Nil(t, err)
	assert.NotNil(t, fileIo)

	n, err := fileIo.Write([]byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	const text = "hello world"
	n, err = fileIo.Write([]byte(text))
	assert.Nil(t, err)
	assert.Equal(t, len(text), n)
}

func TestFileIO_Read(t *testing.T) {
	fileIo, err := fio.NewFileIOManager(filepath.Join(test.TempFilePath))
	defer test.DestroyFile(fileIo)
	assert.Nil(t, err)
	assert.NotNil(t, fileIo)

	const text = "hello world"
	n, err := fileIo.Write([]byte(text))
	assert.Nil(t, err)
	assert.Equal(t, len(text), n)

	buf := make([]byte, len(text))
	n, err = fileIo.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(text), n)
	assert.Equal(t, text, string(buf))
}

func TestFileIO_Sync(t *testing.T) {
	fileIo, err := fio.NewFileIOManager(filepath.Join(test.TempFilePath))
	defer test.DestroyFile(fileIo)
	assert.Nil(t, err)
	assert.NotNil(t, fileIo)

	const text = "hello world"
	n, err := fileIo.Write([]byte(text))
	assert.Nil(t, err)
	assert.Equal(t, len(text), n)

	err = fileIo.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	fileIo, err := fio.NewFileIOManager(filepath.Join(test.TempFilePath))
	defer test.RemoveTempFile()
	assert.Nil(t, err)
	assert.NotNil(t, fileIo)

	err = fileIo.Close()
	assert.Nil(t, err)
}
