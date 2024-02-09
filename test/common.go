package test

import (
	"fairy-kvdb/fio"
	"os"
	"path/filepath"
)

const TempFilePath = "a.data"

var TempDirPath = filepath.Join(os.TempDir(), "fairy-kvdb")

func ClearDatabaseDir(dataDir string) {
	if err := os.RemoveAll(dataDir); err != nil {
		panic(err)
	}
	err := os.Mkdir(dataDir, os.ModePerm)
	if err != nil {
		return
	}
}

func RemoveTempFile() {
	println(TempDirPath)
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
