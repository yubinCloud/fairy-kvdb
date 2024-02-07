package test

import (
	"fairy-kvdb/fio"
	"os"
	"path/filepath"
)

const TempFilePath = "a.data"

var TempDirPath = filepath.Join(os.TempDir(), "fairy-kvdb")

func RemoveTempFile() {
	if err := os.RemoveAll(TempFilePath); err != nil {
		panic(err)
	}
}

func DestroyFile(fileIo *fio.FileIO) {
	err := fileIo.Close()
	if err != nil {
		return
	}
	RemoveTempFile()
}
