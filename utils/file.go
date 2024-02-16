package utils

import (
	"os"
	"path/filepath"
)

// DirSize 获取一个磁盘目录的大小
func DirSize(dirPath string) (int64, error) {
	sz := int64(0)
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			sz += info.Size()
		}
		return nil
	})
	return sz, err
}
