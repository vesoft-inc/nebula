package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/scylladb/go-set/strset"
	"github.com/vesoft-inc/nebula-go/nebula"
	"github.com/vesoft-inc/nebula-go/nebula/meta"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/config"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/ssh"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Restore struct {
	config       config.RestoreConfig
	backend      storage.ExternalStorage
	log          *zap.Logger
	client       *metaclient.MetaClient
	metaFileName string
}

type spaceInfo struct {
	spaceID nebula.GraphSpaceID
	cpDir   string
}

var LeaderNotFoundError = errors.New("not found leader")
var restoreFailed = errors.New("restore failed")

func NewRestore(config config.RestoreConfig, log *zap.Logger) *Restore {
	backend, err := storage.NewExternalStorage(config.BackendUrl, log)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	backend.SetBackupName(config.BackupName)
	return &Restore{config: config, log: log, backend: backend}
}

func (r *Restore) checkPhysicalTopology(info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) error {
	s := strset.New()
	for _, v := range info {
		for _, c := range v.CpDirs {
			s.Add(metaclient.HostaddrToString(c.Host))
		}
	}

	if s.Size() != len(r.config.StorageNodes) {
		return fmt.Errorf("The physical topology of storage must be consistent")
	}
	return nil
}

func (r *Restore) check() error {
	nodes := append(r.config.MetaNodes, r.config.StorageNodes...)
	command := r.backend.CheckCommand()
	return ssh.CheckCommand(command, nodes, r.log)
}

func (r *Restore) downloadMetaFile() error {
	r.metaFileName = r.config.BackupName + ".meta"
	cmdStr := r.backend.RestoreMetaFileCommand(r.metaFileName, "/tmp/")
	r.log.Info("download metafile", zap.Strings("cmd", cmdStr))
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (r *Restore) restoreMetaFile() (*meta.BackupMeta, error) {
	file, err := os.OpenFile("/tmp/"+r.metaFileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryIn := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	m := meta.NewBackupMeta()
	err = m.Read(binaryIn)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (r *Restore) downloadMeta(g *errgroup.Group, file []string) map[string][][]byte {
	sstFiles := make(map[string][][]byte)
	for _, n := range r.config.MetaNodes {
		cmd, files := r.backend.RestoreMetaCommand(file, n.DataDir)
		ipAddr := strings.Split(n.Addrs, ":")
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr[0], n.User, cmd, r.log) })
		var byteFile [][]byte
		for _, f := range files {
			byteFile = append(byteFile, []byte(f))
		}
		sstFiles[n.Addrs] = byteFile
	}
	return sstFiles
}

func (r *Restore) downloadStorage(g *errgroup.Group, info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) map[string]string {
	idMap := make(map[string][]string)
	for gid, bInfo := range info {
		for _, dir := range bInfo.CpDirs {
			idStr := strconv.FormatInt(int64(gid), 10)
			idMap[metaclient.HostaddrToString(dir.Host)] = append(idMap[metaclient.HostaddrToString(dir.Host)], idStr)
		}
	}

	i := 0
	storageIPmap := make(map[string]string)
	for ip, ids := range idMap {
		sNode := r.config.StorageNodes[i]
		r.log.Info("download", zap.String("ip", ip), zap.String("storage", sNode.Addrs))
		cmd := r.backend.RestoreStorageCommand(ip, ids, sNode.DataDir+"/nebula")
		addr := strings.Split(sNode.Addrs, ":")
		storageIPmap[ip] = sNode.Addrs
		g.Go(func() error { return ssh.ExecCommandBySSH(addr[0], sNode.User, cmd, r.log) })
		i++
	}

	for _, s := range r.config.StorageNodes {
		if _, ok := storageIPmap[s.Addrs]; ok {
			delete(storageIPmap, s.Addrs)
			continue
		}
	}

	return storageIPmap
}
func stringToHostAddr(host string) (*nebula.HostAddr, error) {
	ipAddr := strings.Split(host, ":")
	port, err := strconv.ParseInt(ipAddr[1], 10, 32)
	if err != nil {
		return nil, err
	}
	return &nebula.HostAddr{ipAddr[0], nebula.Port(port)}, nil
}

func sendRestoreMeta(addr string, files [][]byte, hostMap []*meta.HostPair, log *zap.Logger) error {

	// retry 3 times if restore failed
	count := 3
	for {
		if count == 0 {
			return restoreFailed
		}
		client := metaclient.NewMetaClient(log)
		err := client.Open(addr)
		if err != nil {
			log.Error("open meta failed", zap.Error(err), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			continue
		}
		defer client.Close()

		restoreReq := meta.NewRestoreMetaReq()
		restoreReq.Hosts = hostMap
		restoreReq.Files = files

		resp, err := client.RestoreMeta(restoreReq)
		if err != nil {
			// maybe we should retry
			log.Error("restore failed", zap.Error(err), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			count--
			continue
		}

		if resp.GetCode() != meta.ErrorCode_SUCCEEDED {
			log.Error("restore failed", zap.String("error code", resp.GetCode().String()), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			count--
			continue
		}
		log.Info("restore succeeded", zap.String("addr", addr))
		return nil
	}
}

func (r *Restore) restoreMeta(sstFiles map[string][][]byte, storageIDMap map[string]string) error {
	r.log.Info("restoreMeta")
	var hostMap []*meta.HostPair

	if len(storageIDMap) != 0 {
		for k, v := range storageIDMap {
			fromAddr, err := stringToHostAddr(k)
			if err != nil {
				return err
			}
			toAddr, err := stringToHostAddr(v)
			if err != nil {
				return err
			}

			pair := &meta.HostPair{fromAddr, toAddr}
			hostMap = append(hostMap, pair)
		}
	}
	g, _ := errgroup.WithContext(context.Background())
	for _, n := range r.config.MetaNodes {
		r.log.Info("will restore meta", zap.String("addr", n.Addrs))
		addr := n.Addrs
		g.Go(func() error { return sendRestoreMeta(addr, sstFiles[n.Addrs], hostMap, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (r *Restore) cleanupOriginal() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.config.StorageNodes {
		cmd := r.backend.RestoreStoragePreCommand(node.DataDir + "/nebula")
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	for _, node := range r.config.MetaNodes {
		cmd := r.backend.RestoreMetaPreCommand(node.DataDir + "/nebula")
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) stopCluster() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.config.StorageNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service stop storaged"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	for _, node := range r.config.MetaNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service stop metad"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startMetaService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.config.MetaNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service start metad &>/dev/null &"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startStorageService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.config.StorageNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service start storaged &>/dev/null &"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, node.User, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) RestoreCluster() error {

	err := r.check()

	if err != nil {
		r.log.Error("restore check failed", zap.Error(err))
		return err
	}

	err = r.downloadMetaFile()
	if err != nil {
		r.log.Error("download meta file failed", zap.Error(err))
		return err
	}

	m, err := r.restoreMetaFile()

	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.checkPhysicalTopology(m.BackupInfo)
	if err != nil {
		r.log.Error("check physical failed", zap.Error(err))
		return err
	}

	err = r.stopCluster()
	if err != nil {
		r.log.Error("stop cluster failed", zap.Error(err))
		return err
	}

	err = r.cleanupOriginal()
	if err != nil {
		r.log.Error("cleanup original failed", zap.Error(err))
		return err
	}

	g, _ := errgroup.WithContext(context.Background())

	var files []string

	for _, f := range m.MetaFiles {
		files = append(files, string(f[:]))
	}

	sstFiles := r.downloadMeta(g, files)
	storageIDMap := r.downloadStorage(g, m.BackupInfo)

	err = g.Wait()
	if err != nil {
		r.log.Error("restore error")
		return err
	}

	err = r.startMetaService()
	if err != nil {
		r.log.Error("start cluster failed", zap.Error(err))
		return err
	}

	time.Sleep(time.Second * 3)

	err = r.restoreMeta(sstFiles, storageIDMap)
	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.startStorageService()
	if err != nil {
		r.log.Error("start storage service failed", zap.Error(err))
		return err
	}

	r.log.Info("restore meta successed")

	return nil

}
