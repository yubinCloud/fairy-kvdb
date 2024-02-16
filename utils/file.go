package utils

import (
	"os"
	"path/filepath"
	"strings"
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

// CopyDir 复制一个目录到另一个目录
// excludes 是需要排除的文件或目录
func CopyDir(src, dst string, excludes []string) error {
	// 目标文件夹如果不存在，则创建
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err := os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}
	// 遍历源文件夹，完成每个文件和文件夹的拷贝
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1) // 这个文件的文件名
		if fileName == "" {
			return nil
		}
		// 过滤 excludes
		for _, e := range excludes {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() { // 如果是目录，则创建目录
			return os.Mkdir(filepath.Join(dst, fileName), info.Mode())
		} else { // 否则就是文件
			fileContent, err := os.ReadFile(filepath.Join(src, fileName))
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dst, fileName), fileContent, info.Mode())
		}
	})
}
