package data

import (
	"fairy-kvdb/data"
	"fairy-kvdb/fio"
	"fairy-kvdb/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	df1, err := data.OpenDataFile(test.TempDirPath, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := data.OpenDataFile(test.TempDirPath, 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
}

func TestDataFile_Write(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = df.Write([]byte("Hi World"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	df, err := data.OpenDataFile(test.TempDirPath, 222, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	// 写入一条记录
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordNormal,
	}
	encBytes1, totalSize1 := data.EncodeLogRecord(rec1)
	err = df.Write(encBytes1)
	assert.Nil(t, err)

	// 读取记录
	record1, recordSize1, err := df.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.NotNil(t, record1)
	assert.Equal(t, rec1, record1)
	assert.Equal(t, totalSize1, recordSize1)

	// 多条 LogRecord 的情况
	rec2 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("yuBin"),
		Type:  data.LogRecordNormal,
	}
	encBytes2, totalSize2 := data.EncodeLogRecord(rec2)
	err = df.Write(encBytes2)
	assert.Nil(t, err)
	record2, recordSize2, err := df.ReadLogRecord(totalSize1)
	assert.Nil(t, err)
	assert.NotNil(t, record2)
	assert.Equal(t, rec2, record2)
	assert.Equal(t, totalSize2, recordSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordDelete,
	}
	encBytes3, totalSize3 := data.EncodeLogRecord(rec3)
	err = df.Write(encBytes3)
	assert.Nil(t, err)
	record3, recordSize3, err := df.ReadLogRecord(totalSize1 + totalSize2)
	assert.Nil(t, err)
	assert.NotNil(t, record3)
	assert.Equal(t, rec3, record3)
	assert.Equal(t, totalSize3, recordSize3)
}
