package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCheckSSH(t *testing.T) {
	addrs := []string{"111"}
	user := "testuser"
	log, _ := zap.NewProduction()
	err := checkSSH(addrs, user, log)
	assert.Error(t, err)
	var addrError addressValidateError
	addrError.Ip = "111"
	assert.Equal(t, err, &addrError)
}
