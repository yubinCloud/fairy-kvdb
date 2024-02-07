package data

import (
	"fairy-kvdb/data"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	df1, err := data.OpenDataFile(test.TempDirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := data.OpenDataFile(test.TempDirPath, 111)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
}

func TestDataFile_Write(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = df.Write([]byte("Hi World"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Sync()
	assert.Nil(t, err)
}
