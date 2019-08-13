package ha

import (
	"context"
	"fmt"
	"net/rpc"

	apiTypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"

	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

// EtcdMgr is the struct to manager etcd.
type EtcdMgr struct {
	config            *config.Config
	client            *clientv3.Client
	leaseTTL          int64
	leaseKeepAliveRsp <-chan *clientv3.LeaseKeepAliveResponse
	LeaseResp         *clientv3.LeaseGrantResponse
	PeerMgr           mgr.PeerMgr
}

const (
	// SupernodeOFF means there is no active supernode.
	SupernodeOFF = ""

	// etcdTimeOut is the etcd client's timeout second.
	etcdTimeOut = 10 * time.Second

	supernodeKeyPrefix = "/standby/supernode/"

	// the signal received from etcd watch.

	// SupernodeChange means the active supernode is off.
	SupernodeChange = 0
	// SupernodeKeep means the active supernode is healthy.
	SupernodeKeep = 1
)

// NewEtcdMgr produces a etcdmgr object.
func NewEtcdMgr(cfg *config.Config, peerMgr mgr.PeerMgr) (*EtcdMgr, error) {
	config := clientv3.Config{
		Endpoints:   cfg.HAConfig,
		DialTimeout: etcdTimeOut,
	}
	// build connection to etcd.
	client, err := clientv3.New(config)
	if err != nil {
		logrus.Errorf("failed to connect to etcd server,err %v", err)
		return nil, err
	}
	return &EtcdMgr{
		leaseTTL: 2,
		config:   cfg,
		client:   client,
		PeerMgr:  peerMgr,
	}, err

}

// SendStandbySupernodesInfo sends standby supernode's info to etcd.and if the standby supernode if off,the etcd will will notice that.
func (etcd *EtcdMgr) SendSupernodesInfo(ctx context.Context, key, ip, pID string, listenPort, downloadPort, rpcPort int, hostName strfmt.Hostname, timeout int64) error {
	var respchan <-chan *clientv3.LeaseKeepAliveResponse
	kv := clientv3.NewKV(etcd.client)
	lease := clientv3.NewLease(etcd.client)
	leaseResp, e := lease.Grant(ctx, timeout)
	value := fmt.Sprintf("%s@%d@%d@%d@%s@%s", ip, listenPort, downloadPort, rpcPort, hostName, pID)
	if _, e = kv.Put(ctx, key, value, clientv3.WithLease(leaseResp.ID)); e != nil {
		logrus.Errorf("failed to put standby supernode's info to etcd as a lease,err %v", e)
		return e
	}
	etcd.LeaseResp = leaseResp
	if respchan, e = lease.KeepAlive(ctx, leaseResp.ID); e != nil {
		logrus.Errorf("failed to send heart beat to etcd to renew the lease %v", e)
		return e
	}
	//deal with the channel full warn
	//TODO(yunfeiyangbuaa):do with this code
	go func() {
		for {
			<-respchan
		}
	}()
	return nil
}

// GetSupenrodesInfo gets supernode info from etcd
func (etcd *EtcdMgr) GetSupenrodesInfo(ctx context.Context, key string) ([]config.SupernodeInfo, error) {
	var (
		nodes  []config.SupernodeInfo
		getRes *clientv3.GetResponse
		e      error
	)
	kv := clientv3.NewKV(etcd.client)
	if getRes, e = kv.Get(ctx, key, clientv3.WithPrefix()); e != nil {
		logrus.Errorf("failed to get the supernode's information,err %v", e)
		return nil, e
	}
	for _, v := range getRes.Kvs {
		splits := strings.Split(string(v.Value), "@")
		if splits[5] == etcd.config.GetSuperPID() {
			continue
		}
		lPort, _ := strconv.Atoi(splits[1])
		dPort, _ := strconv.Atoi(splits[2])
		rPort, _ := strconv.Atoi(splits[3])
		rpcAddress := fmt.Sprintf("%s:%d", splits[0], rPort)
		conn, err := rpc.DialHTTP("tcp", rpcAddress)
		if err != nil {
			logrus.Errorf("failed to connect to the rpc port %s,err: %v", rpcAddress, err)
			return nil, err
		}
		nodes = append(nodes, config.SupernodeInfo{
			IP:           splits[0],
			ListenPort:   lPort,
			DownloadPort: dPort,
			RpcPort:      rPort,
			HostName:     strfmt.Hostname(splits[4]),
			PID:          splits[5],
			RpcClient:    conn,
		})
	}
	etcd.config.SetOtherSupernodes(nodes)
	return nodes, nil
}

// Close closes the tool used to implement supernode ha.
func (etcd *EtcdMgr) Close(ctx context.Context) error {
	var err error
	if err = etcd.client.Close(); err != nil {
		logrus.Errorf("failed to close etcd client,err %v", err)
		return err
	}
	logrus.Info("success to close a etcd client")
	return nil
}

// StopKeepHeartBeat stops sending heart beat to cancel a etcd lease
func (etcd *EtcdMgr) StopKeepHeartBeat(ctx context.Context) (err error) {
	var id *clientv3.LeaseGrantResponse
	if id = etcd.LeaseResp; id == nil {
		return errors.Wrap(err, "etcd lease is nil")
	}
	if _, err := etcd.client.Revoke(ctx, id.ID); err != nil {
		logrus.Errorf("failed to cancel a etcd lease: %v", err)
		return err
	}
	logrus.Info("success to cancel a etcd  lease")
	return nil
}

// WatchStandbySupernodesChange is the progress to watch the etcd,if the value of key prefix changes,supernode will be notified.
func (etcd *EtcdMgr) WatchSupernodesChange(ctx context.Context, key string) error {
	var (
		supernodes []config.SupernodeInfo
		err        error
	)
	if supernodes, err = etcd.GetSupenrodesInfo(ctx, key); err != nil {
		logrus.Errorf("failed to get standby supernode info,err: %v", err)
		return err
	}
	etcd.RegisterOtherSupernodesAsPeer(supernodes)
	watcher := clientv3.NewWatcher(etcd.client)
	watchChan := watcher.Watch(ctx, key, clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			logrus.Infof("success to notice standby supernodes changes,code(0:standby supernode add,1:standby supernode delete) %d", int(event.Type))
			if supernodes, err = etcd.GetSupenrodesInfo(ctx, key); err != nil {
				logrus.Errorf("failed to get standby supernode info,err: %v", err)
				return err
			}
			switch event.Type {
			case SupernodeChange:
				etcd.RegisterOtherSupernodesAsPeer(supernodes)
			case SupernodeKeep:
				etcd.DeRegisterOtherSupernodePeer(supernodes)
			default:
				logrus.Warnf("failed to watch active supernode,unexpected response: %d", int(event.Type))
			}
		}
	}
	return nil
}

func (etcd *EtcdMgr) RegisterOtherSupernodesAsPeer(supernodes []config.SupernodeInfo) {
	for _, node := range supernodes {
		etcd.RegisterSupernodeAsPeer(node)
	}

}

//TODO(yunfeiyangbuaa)add a deregister function
func (etcd *EtcdMgr) DeRegisterOtherSupernodePeer(nodes []config.SupernodeInfo) error {
	logrus.Info("some supernodes are off,should delete the peer")
	return nil
}

func (etcd *EtcdMgr) RegisterSupernodeAsPeer(node config.SupernodeInfo) (err error) {
	if node.PID == "" {
		logrus.Errorf("failed to register a supernode as peer,node info is nil")
		return errors.Wrap(err, "failed to register other supernode as peer,a nil supernode's PID")
	}
	if node.PID == etcd.config.GetSuperPID() {
		return nil
	}
	//try whether this peer is already exist
	if peer, _ := etcd.PeerMgr.Get(context.Background(), node.PID); peer != nil {
		return nil
	}
	peerCreatRequest := &apiTypes.PeerCreateRequest{
		IP:       strfmt.IPv4(node.IP),
		HostName: strfmt.Hostname(node.HostName),
		Port:     int32(node.DownloadPort),
		PeerID:   node.PID,
	}
	_, err = etcd.PeerMgr.Register(context.Background(), peerCreatRequest)
	if err != nil {
		logrus.Errorf("failed to register other supernode %s as a peer,err %v", node.PID, err)
		return err
	}
	logrus.Infof("success to register supernode as peer,peerID is %s", node.PID)
	return nil
}

func (ha *Manager) AddSupernodeCdnResource(task *apiTypes.TaskInfo, node config.SupernodeInfo) error {
	if node.PID == ha.config.GetSuperPID() {
		return nil
	}
	cid := fmt.Sprintf("%s:%s~%s", "cdnnode", node.IP, task.ID)
	path, err := ha.CDNMgr.GetHTTPPath(context.Background(), task.ID)
	if err != nil {
		return err
	}
	if err := ha.DfgetTaskMgr.Add(context.Background(), &apiTypes.DfGetTask{
		CID:       cid[8:],
		Path:      path,
		PeerID:    node.PID,
		PieceSize: task.PieceSize,
		Status:    apiTypes.DfGetTaskStatusWAITING,
		TaskID:    task.ID,
	}); err != nil {
		return errors.Wrapf(err, "failed to add cdn dfgetTask for taskID %s", task.ID)
	}
	ha.ProgressMgr.InitProgress(context.Background(), task.ID, node.PID, cid)
	return nil
}
