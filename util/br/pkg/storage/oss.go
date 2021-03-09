package storage

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type OSSBackedStore struct {
	url           string
	log           *zap.Logger
	backupName    string
	maxConcurrent string
	args          string
}

func NewOSSBackendStore(url string, log *zap.Logger, maxConcurrent int, args string) *OSSBackedStore {
	return &OSSBackedStore{url: url, log: log, maxConcurrent: strconv.Itoa(maxConcurrent), args: args}
}

func (s *OSSBackedStore) SetBackupName(name string) {
	s.backupName = name
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
}

func (s *OSSBackedStore) BackupPreCommand() []string {
	return nil
}

func (s *OSSBackedStore) BackupStorageCommand(src string, host string, spaceID string) string {
	storageDir := s.url + "/" + "storage/" + host + "/" + spaceID + "/"
	return "ossutil cp -r " + src + " " + storageDir + " " + s.args + " -j " + s.maxConcurrent
}

func (s OSSBackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta/"
	return "ossutil cp -r " + filepath.Dir(src[0]) + " " + metaDir + " " + s.args + " -j " + s.maxConcurrent
}

func (s OSSBackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"ossutil", "cp", "-r", src, s.url + "/", "-j", s.maxConcurrent}

	}
	args := strings.Fields(s.args)
	args = append(args, "-r", src, s.url+"/", "-j", s.maxConcurrent)
	args = append([]string{"ossutil", "cp"}, args...)
	return args
}

func (s OSSBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"ossutil", "cp", "-r", s.url + "/" + file, dst, "-j", s.maxConcurrent}
	}
	args := strings.Fields(s.args)
	args = append(args, "-r", s.url+"/"+file, dst, "-j", s.maxConcurrent)
	args = append([]string{"ossutil", "cp"}, args...)
	return args
}

func (s OSSBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	var sstFiles []string
	for _, f := range src {
		file := dst + "/" + f
		sstFiles = append(sstFiles, file)
	}
	return fmt.Sprintf("ossutil cp -r %s %s -j %s %s", metaDir, dst, s.maxConcurrent, s.args), sstFiles
}
func (s OSSBackedStore) RestoreStorageCommand(host string, spaceID []string, dst string) string {
	storageDir := s.url + "/storage/" + host + "/"

	return fmt.Sprintf("ossutil cp -r %s %s -j %s %s", storageDir, dst, s.maxConcurrent, s.args)
}
func (s OSSBackedStore) RestoreMetaPreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s OSSBackedStore) RestoreStoragePreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s OSSBackedStore) URI() string {
	return s.url
}

func (s OSSBackedStore) CheckCommand() string {
	return "ossutil ls " + s.url
}
