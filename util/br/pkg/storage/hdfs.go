package storage

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type HDFSBackedStore struct {
	url        string
	log        *zap.Logger
	backupName string
	args       string
	command    string
}

func NewHDFSBackendStore(url string, log *zap.Logger, maxConcurrent int, args string) *HDFSBackedStore {
	return &HDFSBackedStore{url: url, log: log, args: args}
}

func NewHDFSBackedStore(url string, log *zap.Logger, maxConcurrent int, args string) *HDFSBackedStore {
	return &HDFSBackedStore{url: url, log: log, args: args}
}

func (s *HDFSBackedStore) SetBackupName(name string) {
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
	s.backupName = name
}

func (s HDFSBackedStore) URI() string {
	return s.url
}

func (s HDFSBackedStore) copyCommand(src []string, dir string) string {
	cmdFormat := "hadoop fs -mkdir -p " + dir + " && hadoop fs -copyFromLocal  %s %s " + dir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, s.args, files)
}

func (s *HDFSBackedStore) BackupPreCommand() []string {
	return []string{"hadoop", "fs", "-mkdir", s.url}
}

func (s HDFSBackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta"
	return s.copyCommand(src, metaDir)
}

func (s HDFSBackedStore) BackupStorageCommand(src string, host string, spaceId string) string {
	hosts := strings.Split(host, ":")
	storageDir := s.url + "/" + "storage/" + hosts[0] + "/" + hosts[1] + "/" + spaceId
	data := src + "/data "
	wal := src + "/wal "
	return "hadoop fs -mkdir -p " + storageDir + " && hadoop fs -copyFromLocal " + s.args + " " + data + wal + storageDir
}

func (s HDFSBackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"hadoop", "fs", "-copyFromLocal", src, s.url}
	}
	args := strings.Fields(s.args)
	args = append(args, src, s.url)
	args = append([]string{"hadoop", "fs", "-copyFromLocal"}, args...)
	return args
}

func (s HDFSBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"hadoop", "fs", "-copyToLocal", "-f", s.url + "/" + file, dst}
	}
	args := strings.Fields(s.args)
	args = append(args, s.url+"/"+file, dst)
	args = append([]string{"hadoop", "fs", "-copyToLocal", "-f"}, args...)
	return args
}

func (s HDFSBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	files := ""
	var sstFiles []string
	for _, f := range src {
		file := metaDir + f
		files += file + " "
		dstFile := dst + "/" + f
		sstFiles = append(sstFiles, dstFile)
	}
	return fmt.Sprintf("hadoop fs -copyToLocal -f %s %s %s", files, s.args, dst), sstFiles
}

func (s HDFSBackedStore) RestoreStorageCommand(host string, spaceID []string, dst string) string {
	hosts := strings.Split(host, ":")
	storageDir := s.url + "/storage/" + hosts[0] + "/" + hosts[1] + "/"
	dirs := ""
	for _, id := range spaceID {
		dirs += storageDir + id + " "
	}

	return fmt.Sprintf("hadoop fs -copyToLocal %s %s %s", dirs, s.args, dst)
}

func (s HDFSBackedStore) RestoreMetaPreCommand(dst string) string {
	//cleanup meta
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s HDFSBackedStore) RestoreStoragePreCommand(dst string) string {
	//cleanup storage
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s HDFSBackedStore) CheckCommand() string {
	return "hadoop fs -ls " + s.url
}
