package fairy_kvdb

import "errors"

var (
	ErrorKeyEmpty               = errors.New("key is empty")
	ErrorIndexUpdateFailed      = errors.New("index update failed")
	ErrorKeyNotFound            = errors.New("key not found")
	ErrorDataFileNotFound       = errors.New("data file not found")
	ErrorDataFileCorrupt        = errors.New("data file corrupt")
	ErrorExceedMaxWriteBatchNum = errors.New("exceed max write batch num")
	ErrorMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrorDatabaseIsUsing        = errors.New("the database directory is using by another process")
)
