package ha

import (
	"context"
	"fmt"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	apiTypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/pkg/errors"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

// EtcdMgr is the struct to manager etcd.
type EtcdMgr struct {
	config        *config.Config
	client        *clientv3.Client
	LeaseResp     *clientv3.LeaseGrantResponse
	preSupernodes []config.SupernodeInfo
	PeerMgr       mgr.PeerMgr
	ProgressMgr   mgr.ProgressMgr
}

const (
	// etcdTimeOut is the etcd client's timeout second.
	etcdTimeOut = 10 * time.Second

	supernodeKeyPrefix = "/standby/supernode/"

	// SupernodeDEL means some supernode is off.
	SupernodeDEL = 0
	// SupernodeADD means some supernode join in the supernode ha cluster.
	SupernodeADD = 1
)

// NewEtcdMgr produces a etcdmgr object.
func NewEtcdMgr(cfg *config.Config, peerMgr mgr.PeerMgr, progressMgr mgr.ProgressMgr) (*EtcdMgr, error) {
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
		config:      cfg,
		client:      client,
		PeerMgr:     peerMgr,
		ProgressMgr: progressMgr,
	}, err

}

// SendSupernodesInfo send supernode info to other supernode.
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
	//TODO(yunfeiyangbuaa):do with this code,because it is useless
	go func() {
		for {
			<-respchan
		}
	}()
	return nil
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

// WatchSupernodesChange is the progress to watch the etcd,if the value of key prefix changes,supernode will be notified.
func (etcd *EtcdMgr) WatchSupernodesChange(ctx context.Context, key string) error {
	//when start supernode,get supernode info
	if _, err := etcd.getSupenrodesInfo(ctx, key); err != nil {
		logrus.Errorf("failed to get standby supernode info,err: %v", err)
		return err
	}
	etcd.registerOtherSupernodesAsPeer(ctx)
	watcher := clientv3.NewWatcher(etcd.client)
	watchChan := watcher.Watch(ctx, key, clientv3.WithPrefix())

	//after supernode start,if other supernode changes,do with it
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			logrus.Infof("success to notice supernodes changes,code(1:supernode add,0:supernode delete) %d", int(event.Type))
			if _, err := etcd.getSupenrodesInfo(ctx, key); err != nil {
				logrus.Errorf("failed to get standby supernode info,err: %v", err)
				return err
			}
			switch event.Type {
			case SupernodeDEL:
				etcd.registerOtherSupernodesAsPeer(ctx)
			case SupernodeADD:
				etcd.deRegisterOtherSupernodePeer(ctx)
			default:
				logrus.Warnf("failed to watch active supernode,unexpected response: %d", int(event.Type))
			}
		}
	}
	return nil
}

// GetSupenrodesInfo gets supernode info from etcd
func (etcd *EtcdMgr) getSupenrodesInfo(ctx context.Context, key string) ([]config.SupernodeInfo, error) {
	var (
		nodes  []config.SupernodeInfo
		getRes *clientv3.GetResponse
		e      error
	)
	etcd.preSupernodes = etcd.config.GetOtherSupernodeInfo()
	kv := clientv3.NewKV(etcd.client)
	if getRes, e = kv.Get(ctx, key, clientv3.WithPrefix()); e != nil {
		logrus.Errorf("failed to get other supernode's information,err %v", e)
		return nil, e
	}
	for _, v := range getRes.Kvs {
		splits := strings.Split(string(v.Value), "@")
		// if the supernode is itself,skip
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
			RPCPort:      rPort,
			HostName:     strfmt.Hostname(splits[4]),
			PID:          splits[5],
			RPCClient:    conn,
		})
	}
	etcd.config.SetOtherSupernodeInfo(nodes)
	return nodes, nil
}

// registerOtherSupernodesAsPeer registers all other supernodes as a peer
func (etcd *EtcdMgr) registerOtherSupernodesAsPeer(ctx context.Context) {
	supernodes := etcd.config.GetOtherSupernodeInfo()
	for _, node := range etcd.config.GetOtherSupernodeInfo() {
		if err := etcd.registerSupernodeAsPeer(ctx, node); err != nil {

			// if failed,delete the supernode in config
			var newSupernodes []config.SupernodeInfo
			for k, v := range supernodes {
				if v == node {
					kk := k + 1
					newSupernodes = append(supernodes[:k], supernodes[kk:]...)
				}
			}
			etcd.config.SetOtherSupernodeInfo(newSupernodes)
		}
	}

}

// registerSupernodeAsPeer registers supernode as a peer
func (etcd *EtcdMgr) registerSupernodeAsPeer(ctx context.Context, node config.SupernodeInfo) (err error) {
	if node.PID == "" {
		logrus.Errorf("failed to register a supernode as peer,node PID is nil")
		return errors.Wrap(err, "failed to register other supernode as peer,a nil supernode's PID")
	}
	if node.PID == etcd.config.GetSuperPID() {
		return nil
	}
	// try whether this peer is already exist
	if peer, err := etcd.PeerMgr.Get(ctx, node.PID); peer != nil || err != nil {
		return nil
	}
	peerCreatRequest := &apiTypes.PeerCreateRequest{
		IP:       strfmt.IPv4(node.IP),
		HostName: strfmt.Hostname(node.HostName),
		Port:     int32(node.DownloadPort),
		PeerID:   node.PID,
	}
	_, err = etcd.PeerMgr.Register(ctx, peerCreatRequest)
	if err != nil {
		logrus.Errorf("failed to register other supernode %s as a peer,err %v", node.PID, err)
		return err
	}
	logrus.Infof("success to register supernode as peer,peerID is %s", node.PID)
	return nil
}

// deRegisterOtherSupernodePeer find and deregister all offline supernode
// TODO(yunfeiyangbuaa)modify the code,make it more efficiency
func (etcd *EtcdMgr) deRegisterOtherSupernodePeer(ctx context.Context) {
	for _, pre := range etcd.preSupernodes {
		mark := false
		for _, now := range etcd.config.GetOtherSupernodeInfo() {
			if pre.PID == now.PID {
				mark = true
				break
			}
		}
		if mark == false {
			etcd.deRegisterSupernodePeer(ctx, pre)
			logrus.Info("supernodes %s are off,should delete the peer", pre.PID)
		}
	}
}

// deRegisterSupernodePeer deregister a supernode peer when this supernode is off
func (etcd *EtcdMgr) deRegisterSupernodePeer(ctx context.Context, node config.SupernodeInfo) {
	if err := etcd.ProgressMgr.DeletePeerStateByPeerID(ctx, node.PID); err != nil {
		logrus.Errorf("failed to delete supernode peer peerState  %s,err: %v", node.PID, err)
	}
	if err := etcd.PeerMgr.DeRegister(ctx, node.PID); err != nil {
		logrus.Errorf("failed to delete supernode peer peerMgr %s,err: %v", node.PID, err)
	}
}
