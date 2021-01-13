package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vesoft-inc/nebula-storage/util/br/pkg/remote"
	"go.uber.org/zap"
)

type addressValidateError struct {
	Ip string
}

type sshValidateError struct {
	err error
}

func (e sshValidateError) Error() string {
	return e.err.Error()
}

func (e addressValidateError) Error() string {
	return fmt.Sprintf("The ip address(%s) must contain the port", e.Ip)
}

func (e sshValidateError) UnWrap() error {
	return e.err
}

func checkSSH(addr string, user string, log *zap.Logger) error {
	log.Info("checking ssh", zap.String("addr", addr))
	ipAddr := strings.Split(addr, ":")
	if len(ipAddr) != 2 {
		return &addressValidateError{addr}
	}
	client, err := remote.NewClient(ipAddr[0], user, log)
	if err != nil {
		log.Error("must enable SSH tunneling")
		return &sshValidateError{err}
	}
	client.Close()
	return nil
}

func checkPathAbs(path string) bool {
	return filepath.IsAbs(path)
}
