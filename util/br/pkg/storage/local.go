package storage

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type LocalBackedStore struct {
	dir        string
	backupName string
	log        *zap.Logger
	args       string
}

func NewLocalBackedStore(dir string, log *zap.Logger, maxConcurrent int, args string) *LocalBackedStore {
	return &LocalBackedStore{dir: dir, log: log, args: args}
}

func (s *LocalBackedStore) SetBackupName(name string) {
	s.backupName = name
	s.dir += "/" + s.backupName
}

func (s LocalBackedStore) URI() string {
	return s.dir
}

func (s LocalBackedStore) copyCommand(src []string, dir string) string {
	cmdFormat := "mkdir -p " + dir + " && cp -rf %s %s " + dir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, s.args, files)
}

func (s *LocalBackedStore) BackupPreCommand() []string {
	return []string{"mkdir", s.dir}
}

func (s LocalBackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.dir + "/" + "meta"
	return s.copyCommand(src, metaDir)
}

func (s LocalBackedStore) BackupStorageCommand(src string, host string, spaceId string) string {
	storageDir := s.dir + "/" + "storage/" + host + "/" + spaceId
	data := src + "/data "
	wal := src + "/wal "
	return "mkdir -p " + storageDir + " && cp -rf " + s.args + " " + data + wal + storageDir
}

func (s LocalBackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"cp", src, s.dir}
	}
	args := strings.Fields(s.args)
	args = append(args, src, s.dir)
	args = append([]string{"cp"}, args...)
	return args
}

func (s LocalBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"cp", s.dir + "/" + file, dst}
	}
	args := strings.Fields(s.args)
	args = append(args, s.dir+"/"+file, dst)
	args = append([]string{"cp"}, args...)
	return args
}

func (s LocalBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.dir + "/" + "meta/"
	files := ""
	var sstFiles []string
	for _, f := range src {
		file := metaDir + f
		files += file + " "
		dstFile := dst + "/" + f
		sstFiles = append(sstFiles, dstFile)
	}
	return fmt.Sprintf("cp -rf %s %s %s", files, s.args, dst), sstFiles
}

func (s LocalBackedStore) RestoreStorageCommand(host string, spaceID []string, dst string) string {
	storageDir := s.dir + "/storage/" + host + "/"
	dirs := ""
	for _, id := range spaceID {
		dirs += storageDir + id + " "
	}

	return fmt.Sprintf("cp -rf %s %s %s", dirs, s.args, dst)
}

func (s LocalBackedStore) RestoreMetaPreCommand(dst string) string {
	//cleanup meta
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s LocalBackedStore) RestoreStoragePreCommand(dst string) string {
	//cleanup storage
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s LocalBackedStore) CheckCommand() string {
	return "ls " + s.dir
}
