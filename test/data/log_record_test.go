package data

import (
	"fairy-kvdb/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况下
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordNormal,
	}
	encBytes1, totalSize1 := data.EncodeLogRecord(rec1)
	assert.NotNil(t, encBytes1)
	assert.Equal(t, byte(data.LogRecordNormal), encBytes1[4])
	assert.Equal(t, []byte("name"), encBytes1[8:12])
	assert.Equal(t, []byte("zhangSan"), encBytes1[12:20])
	assert.Equal(t, int64(20), totalSize1) // 20 = 4 + 1 + 1 + 1 + 1 + 4 + 8

	// value 为空的情况
	rec2 := &data.LogRecord{
		Key:  []byte("name"),
		Type: data.LogRecordNormal,
	}
	encBytes2, totalSize2 := data.EncodeLogRecord(rec2)
	assert.NotNil(t, encBytes2)
	assert.Equal(t, byte(data.LogRecordNormal), encBytes2[4])
	assert.Equal(t, []byte("name"), encBytes2[8:12])
	assert.Equal(t, int64(12), totalSize2)

	// 对 Deleted type 情况的测试
	rec3 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordDelete,
	}
	encBytes3, totalSize3 := data.EncodeLogRecord(rec3)
	assert.NotNil(t, encBytes3)
	assert.Equal(t, byte(data.LogRecordDelete), encBytes3[4])
	assert.Equal(t, []byte("name"), encBytes3[8:12])
	assert.Equal(t, []byte("zhangSan"), encBytes3[12:20])
	assert.Equal(t, int64(20), totalSize3)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// 正常情况下
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordNormal,
	}
	encBytes1, totalSize1 := data.EncodeLogRecord(rec1)
	headerSize1 := totalSize1 - int64(len(rec1.Key)) - int64(len(rec1.Value))
	decodedHeader1, decodedLength := data.DecodeLogRecordHeader(encBytes1[:headerSize1])
	assert.Equal(t, headerSize1, decodedLength)
	assert.Equal(t, uint32(2008391071), decodedHeader1.Crc)
	assert.Equal(t, data.LogRecordNormal, decodedHeader1.RecType)
	assert.Equal(t, uint32(4), decodedHeader1.KeySize)
	assert.Equal(t, uint32(8), decodedHeader1.ValueSize)

	// value 为空的情况
	rec2 := &data.LogRecord{
		Key:  []byte("name"),
		Type: data.LogRecordNormal,
	}
	encBytes2, totalSize2 := data.EncodeLogRecord(rec2)
	headerSize2 := totalSize2 - int64(len(rec2.Key))
	decodedHeader2, decodedLength2 := data.DecodeLogRecordHeader(encBytes2[:headerSize2])
	assert.Equal(t, headerSize2, decodedLength2)
	assert.Equal(t, data.LogRecordNormal, decodedHeader2.RecType)
	assert.Equal(t, uint32(4), decodedHeader2.KeySize)
	assert.Equal(t, uint32(0), decodedHeader2.ValueSize)

	// 对 Deleted type 情况的测试
	rec3 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordDelete,
	}
	encBytes3, totalSize3 := data.EncodeLogRecord(rec3)
	headerSize3 := totalSize3 - int64(len(rec3.Key)) - int64(len(rec3.Value))
	decodedHeader3, decodedLength3 := data.DecodeLogRecordHeader(encBytes3[:headerSize3])
	assert.Equal(t, headerSize3, decodedLength3)
	assert.Equal(t, data.LogRecordDelete, decodedHeader3.RecType)
	assert.Equal(t, uint32(4), decodedHeader3.KeySize)
	assert.Equal(t, uint32(8), decodedHeader3.ValueSize)
}

func TestComputeCRC(t *testing.T) {
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("zhangSan"),
		Type:  data.LogRecordNormal,
	}
	encBytes1, totalSize1 := data.EncodeLogRecord(rec1)
	headerSize1 := totalSize1 - int64(len(rec1.Key)) - int64(len(rec1.Value))
	crc := data.ComputeCRC(rec1, encBytes1[4:headerSize1])
	assert.Equal(t, uint32(2008391071), crc)
}
