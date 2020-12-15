package metaclient

import (
	"strconv"

	"github.com/vesoft-inc/nebula-go/nebula"
)

func HostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + strconv.Itoa(int(host.Port))
}
