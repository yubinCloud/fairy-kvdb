package fairy_kvdb

import (
	"encoding/binary"
	"fairy-kvdb/data"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

const (
	mergeDirName      = "-merge"
	mergeFinRecordKey = "merge-finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}
	// 检查是否有其他进程正在 merge
	if ok := atomic.CompareAndSwapInt32(&db.isMerging, 0, 1); !ok {
		return ErrorMergeIsProgress
	}
	defer atomic.StoreInt32(&db.isMerging, 0)
	db.mu.Lock()
	// 持久化当前活跃文件，并新建一个活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录一下最近没有参与 merge 的文件 ID
	nonMergedFid := db.activeFile.FileId
	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock() // 之后的操作对需要进行 merge 的文件不产生影响，所以可以把锁释放掉

	// 待 merge 的文件从小到大进行排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergeDir := db.getMergeDir()
	// 如果目录已经存在，说明发生过 merge，需要清理掉
	if _, err := os.Stat(mergeDir); err == nil {
		if err := os.RemoveAll(mergeDir); err != nil {
			return err
		}
	}
	// 创建 merge 目录
	if err := os.MkdirAll(mergeDir, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时的 DB 实例
	mergeOptions := db.options
	mergeOptions.DataDir = mergeDir
	mergeOptions.SyncEveryWrite = false
	mergeDb, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 打开 Hint 文件来存储索引
	hintFile, err := data.OpenHintFile(mergeDir)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			record, recordSize, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			recordPos := db.index.Get(record.Key)
			// 将 record 的位置与 index 中存储的 recordPos 进行比较，如果有效（两者相等）则重写入 mergeDb 中
			if recordPos != nil && recordPos.Fid == dataFile.FileId && recordPos.Offset == offset {
				record.Type = data.LogRecordNormal
				record.Btsn = data.NoTxnBTSN
				pos, err := mergeDb.appendLogRecord(record)
				if err != nil {
					return err
				}
				// 将当前位置索引写到 Hint 文件中
				if err = hintFile.WriteHintRecord(record.Key, pos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += recordSize
		}
	}
	// sync Hint 文件
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDb.Sync(); err != nil {
		return err
	}
	// 写标识 merge 结束的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergeDir)
	if err != nil {
		return err
	}
	mergeFinRecordValue := make([]byte, 4)
	binary.BigEndian.PutUint32(mergeFinRecordValue, nonMergedFid)
	mergeFinRecord := data.LogRecord{ // 用于记录本次 merge 的结束位置
		Key:   []byte(mergeFinRecordKey),
		Value: mergeFinRecordValue,
		Type:  data.LogRecordNormal,
		Btsn:  data.NoTxnBTSN,
	}
	encodedMergeFinRecord, _ := data.EncodeLogRecord(&mergeFinRecord)
	if err := mergeFinishedFile.Write(encodedMergeFinRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取用于存放 merge 文件的目录
// example:
//   - DatDir: /user/home/fairy-kvdb
//   - MergePath: /user/home/fairy-kvdb-merge
func (db *DB) getMergeDir() string {
	return filepath.Join(db.options.DataDir, mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergeDir()
	// 检查 merge 目录是否存在，不存在的话直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找用于标识 merge 结束的文件，判断 merge 是否已经处理完成了
	mergeFinished := false
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}
	// 获取最近没有参与 merge 的文件 ID
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	// 删除旧的数据文件（也就是已经 merge 过的数据文件）
	for fileId := uint32(0); fileId < nonMergeFileId; fileId++ {
		filePath := data.GetDataFilePath(mergePath, fileId)
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录中（也就是将 merge 目录下的数据文件重命名到数据目录下）
	for _, filename := range mergeFileNames {
		srcPath := filepath.Join(mergePath, filename)          // merge 目录下的数据文件
		dstPath := filepath.Join(db.options.DataDir, filename) // 数据目录下的数据文件
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// 获取 merge 完成文件中记录的最近没有参与 merge 的文件 ID
func (db *DB) getNonMergeFileId(mergeDir string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinishedFile(mergeDir)
	if err != nil {
		return 0, err
	}
	defer mergeFinFile.Close()
	mergeFinRecord, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFid := binary.BigEndian.Uint32(mergeFinRecord.Value)
	return nonMergeFid, nil
}

// 从 Hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 检查 Hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DataDir, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	// 打开 Hint 文件
	hintFile, err := data.OpenHintFile(db.options.DataDir)
	if err != nil {
		return err
	}
	// 从 Hint 文件中读取索引
	var offset int64 = 0
	for {
		record, recordSize, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)
		offset += recordSize
	}
	return nil
}
