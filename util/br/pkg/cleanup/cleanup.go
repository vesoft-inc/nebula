package cleanup

import (
	"errors"

	"github.com/vesoft-inc/nebula-storage/util/br/pkg/config"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-go/nebula/meta"
	"go.uber.org/zap"
)

type Cleanup struct {
	log    *zap.Logger
	config config.CleanupConfig
}

var LeaderNotFoundError = errors.New("not found leader")
var CleanupError = errors.New("cleanup failed")

func NewCleanup(config config.CleanupConfig, log *zap.Logger) *Cleanup {
	return &Cleanup{log, config}
}

func (c *Cleanup) dropBackup() (*meta.ExecResp, error) {
	addr := c.config.MetaServer[0]
	backupName := []byte(c.config.BackupName[:])

	for {
		client := metaclient.NewMetaClient(c.log)
		err := client.Open(addr)
		if err != nil {
			return nil, err
		}

		snapshot := meta.NewDropSnapshotReq()
		snapshot.Name = backupName
		defer client.Close()

		resp, err := client.DropBackup(snapshot)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != meta.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != meta.ErrorCode_SUCCEEDED {
			c.log.Error("cleanup failed", zap.String("error code", resp.GetCode().String()))
			return nil, CleanupError
		}

		if resp.GetCode() == meta.ErrorCode_SUCCEEDED {
			return resp, nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return nil, LeaderNotFoundError
		}

		c.log.Info("leader changed", zap.String("leader", leader.String()))
		addr = metaclient.HostaddrToString(leader)
	}
}

func (c *Cleanup) Run() error {
	_, err := c.dropBackup()
	if err != nil {
		c.log.Error("cleanup failed", zap.Error(err))
		return err
	}
	c.log.Info("cleanup finished", zap.String("backupname", c.config.BackupName))
	return nil
}
