package fairy_kvdb

import "errors"

var (
	ErrorKeyEmpty               = errors.New("key is empty")
	ErrorIndexUpdateFailed      = errors.New("index update failed")
	ErrorKeyNotFound            = errors.New("key not found")
	ErrorDataFileNotFound       = errors.New("data file not found")
	ErrorDataFileCorrupt        = errors.New("data file corrupt")
	ErrorInvalidCRC             = errors.New("invalid crc")
	ErrorExceedMaxWriteBatchNum = errors.New("exceed max write batch num")
)
