package backup

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/vesoft-inc/nebula-go/nebula"
	"github.com/vesoft-inc/nebula-go/nebula/meta"

	"github.com/vesoft-inc/nebula-storage/util/br/pkg/config"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/remote"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/storage"
)

var defaultTimeout time.Duration = 120 * time.Second
var tmpDir = "/tmp/"

type BackupError struct {
	msg string
	Err error
}

type spaceInfo struct {
	spaceID       nebula.GraphSpaceID
	checkpointDir string
}

var LeaderNotFoundError = errors.New("not found leader")
var backupFailed = errors.New("backup failed")

func (e *BackupError) Error() string {
	return e.msg + e.Err.Error()
}

type Backup struct {
	config         config.BackupConfig
	metaNodeMap    map[string]config.NodeInfo
	storageNodeMap map[string]config.NodeInfo
	metaLeader     config.NodeInfo
	backendStorage storage.ExternalStorage
	log            *zap.Logger
	metaFileName   string
}

func NewBackupClient(cf config.BackupConfig, log *zap.Logger) *Backup {
	backend, err := storage.NewExternalStorage(cf.BackendUrl, log, cf.MaxConcurrent, cf.CommandArgs)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	return &Backup{config: cf, backendStorage: backend, log: log}
}

func (b *Backup) dropBackup(name []byte) (*meta.ExecResp, error) {
	addr := b.metaLeader.Addrs

	client := metaclient.NewMetaClient(b.log)
	err := client.Open(addr)
	if err != nil {
		return nil, err
	}

	snapshot := meta.NewDropSnapshotReq()
	snapshot.Name = name
	defer client.Close()

	resp, err := client.DropBackup(snapshot)
	if err != nil {
		return nil, err
	}

	if resp.GetCode() != meta.ErrorCode_SUCCEEDED {
		return nil, fmt.Errorf("drop backup failed %d", resp.GetCode())
	}

	return resp, nil
}

func (b *Backup) createBackup() (*meta.CreateBackupResp, error) {
	node := b.config.MetaNodes
	addr := node[0].Addrs
	b.metaLeader = node[0]

	for {
		client := metaclient.NewMetaClient(b.log)
		err := client.Open(addr)
		if err != nil {
			return nil, err
		}

		backupReq := meta.NewCreateBackupReq()
		defer client.Close()
		if len(b.config.SpaceNames) != 0 {
			for _, name := range b.config.SpaceNames {
				backupReq.Spaces = append(backupReq.Spaces, []byte(name))
			}
		}

		resp, err := client.CreateBackup(backupReq)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != meta.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != meta.ErrorCode_SUCCEEDED {
			b.log.Error("backup failed", zap.String("error code", resp.GetCode().String()))
			return nil, backupFailed
		}

		if resp.GetCode() == meta.ErrorCode_SUCCEEDED {
			return resp, nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return nil, LeaderNotFoundError
		}

		b.log.Info("leader changed", zap.String("leader", leader.String()))
		addr = metaclient.HostaddrToString(leader)
		b.metaLeader = b.metaNodeMap[addr]
	}
}

func (b *Backup) writeMetadata(meta *meta.BackupMeta) error {
	b.metaFileName = tmpDir + string(meta.BackupName[:]) + ".meta"

	file, err := os.OpenFile(b.metaFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryOut := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	var absMetaFiles [][]byte
	for _, files := range meta.MetaFiles {
		f := filepath.Base(string(files[:]))
		absMetaFiles = append(absMetaFiles, []byte(f))
	}
	meta.MetaFiles = absMetaFiles
	err = meta.Write(binaryOut)
	if err != nil {
		return err
	}
	binaryOut.Flush()
	return nil
}

func (b *Backup) Init() {
	metaNodeMap := make(map[string]config.NodeInfo)
	for _, node := range b.config.MetaNodes {
		metaNodeMap[node.Addrs] = node
	}
	b.metaNodeMap = metaNodeMap

	storageNodeMap := make(map[string]config.NodeInfo)
	for _, node := range b.config.StorageNodes {
		storageNodeMap[node.Addrs] = node
	}

	b.storageNodeMap = storageNodeMap
}

func (b *Backup) check() error {
	nodes := append(b.config.MetaNodes, b.config.StorageNodes...)
	command := b.backendStorage.CheckCommand()
	return remote.CheckCommand(command, nodes, b.log)
}

func (b *Backup) BackupCluster() error {
	b.log.Info("start backup nebula cluster")
	err := b.check()
	if err != nil {
		b.log.Error("check failed", zap.Error(err))
		return err
	}

	b.Init()
	resp, err := b.createBackup()
	if err != nil {
		b.log.Error("backup cluster failed", zap.Error(err))
		return err
	}

	meta := resp.GetMeta()
	err = b.uploadAll(meta)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backup) uploadMeta(g *errgroup.Group, files []string) {

	b.log.Info("will upload meta", zap.Int("sst file count", len(files)))
	cmd := b.backendStorage.BackupMetaCommand(files)
	b.log.Info("start upload meta", zap.String("addr", b.metaLeader.Addrs))
	ipAddr := strings.Split(b.metaLeader.Addrs, ":")
	func(addr string, user string, cmd string, log *zap.Logger) {
		g.Go(func() error {
			client, err := remote.NewClient(addr, user, log)
			if err != nil {
				return err
			}
			defer client.Close()
			return client.ExecCommandBySSH(cmd)
		})
	}(ipAddr[0], b.metaLeader.User, cmd, b.log)
}

func (b *Backup) uploadStorage(g *errgroup.Group, dirs map[string][]spaceInfo) error {
	b.log.Info("uploadStorage", zap.Int("dirs length", len(dirs)))
	for k, v := range dirs {
		b.log.Info("start upload storage", zap.String("addr", k))
		idMap := make(map[string]string)
		for _, info := range v {
			idStr := strconv.FormatInt(int64(info.spaceID), 10)
			idMap[idStr] = info.checkpointDir
		}

		ipAddrs := strings.Split(k, ":")
		b.log.Info("uploadStorage idMap", zap.Int("idMap length", len(idMap)))
		clients, err := remote.NewClientPool(ipAddrs[0], b.storageNodeMap[k].User, b.log, b.config.MaxSSHConnections)
		if err != nil {
			b.log.Error("new clients failed", zap.Error(err))
			return err
		}
		i := 0
		//We need to limit the number of ssh connections per storage node
		for id2, cp := range idMap {
			cmd := b.backendStorage.BackupStorageCommand(cp, k, id2)
			if i >= len(clients) {
				i = 0
			}
			client := clients[i]
			func(client *remote.Client, cmd string) {
				g.Go(func() error {
					return client.ExecCommandBySSH(cmd)
				})
			}(client, cmd)
			i++
		}
	}
	return nil
}

func (b *Backup) uploadMetaFile() error {
	cmdStr := b.backendStorage.BackupMetaFileCommand(b.metaFileName)
	b.log.Info("will upload metafile", zap.Strings("cmd", cmdStr))

	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) execPreCommand(backupName string) error {
	b.backendStorage.SetBackupName(backupName)
	cmdStr := b.backendStorage.BackupPreCommand()
	if cmdStr == nil {
		return nil
	}

	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) uploadAll(meta *meta.BackupMeta) error {
	//upload meta
	g, _ := errgroup.WithContext(context.Background())

	err := b.execPreCommand(string(meta.GetBackupName()[:]))
	if err != nil {
		return err
	}

	var metaFiles []string
	for _, f := range meta.GetMetaFiles() {
		fileName := string(f[:])
		if !filepath.IsAbs(fileName) {
			root := b.metaLeader.RootDir
			fileName = filepath.Join(root, fileName)
		}
		metaFiles = append(metaFiles, string(fileName))
	}
	b.uploadMeta(g, metaFiles)
	//upload storage
	storageMap := make(map[string][]spaceInfo)
	for k, v := range meta.GetBackupInfo() {
		for _, f := range v.GetCpDirs() {
			dir := string(f.CheckpointDir)
			if !filepath.IsAbs(dir) {
				root := b.storageNodeMap[metaclient.HostaddrToString(f.Host)].RootDir
				dir = filepath.Join(root, dir)
			}
			cpDir := spaceInfo{k, dir}
			storageMap[metaclient.HostaddrToString(f.Host)] = append(storageMap[metaclient.HostaddrToString(f.Host)], cpDir)
		}
	}
	err = b.uploadStorage(g, storageMap)
	if err != nil {
		return err
	}

	err = g.Wait()
	if err != nil {
		b.log.Error("upload error", zap.Error(err))
		return err
	}
	// write the meta for this backup to local

	err = b.writeMetadata(meta)
	if err != nil {
		b.log.Error("write the meta file failed", zap.Error(err))
		return err
	}
	b.log.Info("write meta data finished")
	// upload meta file
	err = b.uploadMetaFile()
	if err != nil {
		b.log.Error("upload meta file failed", zap.Error(err))
		return err
	}

	_, err = b.dropBackup(meta.GetBackupName())
	if err != nil {
		b.log.Error("drop backup failed", zap.Error(err))
	}

	b.log.Info("backup nebula cluster finished", zap.String("backupName", string(meta.GetBackupName()[:])))

	return nil
}
