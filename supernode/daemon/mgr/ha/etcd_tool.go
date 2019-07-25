package ha

import (
	"context"
	"fmt"
	"time"

	"github.com/dragonflyoss/Dragonfly/supernode/config"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

//EtcdMgr is the struct to manager etcd.
type EtcdMgr struct {
	config            clientv3.Config
	client            *clientv3.Client
	leaseTTL          int64
	leaseKeepAliveRsp <-chan *clientv3.LeaseKeepAliveResponse
	hostIP            string
	activeLeaseResp   *clientv3.LeaseGrantResponse
	standbyLeaseResp  *clientv3.LeaseGrantResponse
}

const (
	//ActiveSupernodeOFF means there is no active supernode.
	ActiveSupernodeOFF = ""
	//ActiveSupernodeChange means active supernode change to standby supernode because of unhealthy.
	ActiveSupernodeChange = 0
	//ActiveSupernodeKeep means active supernode is health.
	ActiveSupernodeKeep = 1
)

//NewEtcdMgr produce a etcdmgr object.
func NewEtcdMgr(cfg *config.Config) (*EtcdMgr, error) {
	config := clientv3.Config{
		Endpoints:   cfg.HAConfig,
		DialTimeout: 10 * time.Second,
	}
	// build connection to etcd.
	client, err := clientv3.New(config)
	return &EtcdMgr{
		hostIP: cfg.AdvertiseIP,
		config: config,
		client: client,
	}, err
}

//WatchActiveChange is the progress to watch the etcd,if the value of key /lock/active changes,supernode will be notified.
func (etcd *EtcdMgr) WatchActiveChange(messageChannel chan string) {
	var watchStartRevision int64
	watcher := clientv3.NewWatcher(etcd.client)
	watchChan := watcher.Watch(context.TODO(), "/lock/active", clientv3.WithRev(watchStartRevision))
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case ActiveSupernodeChange:
				messageChannel <- "string(event.Kv.Value)"
			case ActiveSupernodeKeep:
				messageChannel <- ActiveSupernodeOFF
			}
		}
	}
}

//ObtainActiveInfo obtain the active supernode's information from etcd.
func (etcd *EtcdMgr) ObtainActiveInfo(key string) (string, error) {
	kv := clientv3.NewKV(etcd.client)
	var (
		getRes *clientv3.GetResponse
		err    error
	)
	if getRes, err = kv.Get(context.TODO(), key); err != nil {
		logrus.Errorf("failed to get the active supernode's info: %v", err)
	}
	var value string
	for _, v := range getRes.Kvs {
		value = string(v.Value)
	}
	return value, err
}

//ActiveResureItsStatus keep look on the lease's renew response.
func (etcd *EtcdMgr) ActiveResureItsStatus() {
	for {
		select {
		case keepResp := <-etcd.leaseKeepAliveRsp:
			if keepResp == nil {
				logrus.Info("failed to renew the etcd lease")
				return
			}
		}
	}
}

//TryBeActive try to change the supernode's status from standby to active.
func (etcd *EtcdMgr) TryBeActive() (bool, string, error) {
	kv := clientv3.NewKV(etcd.client)
	//make a lease to obtain a lock
	lease := clientv3.NewLease(etcd.client)
	leaseResp, err := lease.Grant(context.TODO(), etcd.leaseTTL)
	if err != nil {
		logrus.Errorf("failed to create a etcd lease: %v", err)
	}
	keepRespChan, err := lease.KeepAlive(context.TODO(), leaseResp.ID)
	etcd.leaseKeepAliveRsp = keepRespChan
	if err != nil {
		logrus.Errorf("failed to create etcd.leaseKeepAliveRsp: %v", err)
	}
	etcd.activeLeaseResp = leaseResp
	//if the lock is available,get the lock.
	//else read the lock
	txn := kv.Txn(context.TODO())
	txn.If(clientv3.Compare(clientv3.CreateRevision("/lock/active"), "=", 0)).
		Then(clientv3.OpPut("/lock/active", etcd.hostIP, clientv3.WithLease(leaseResp.ID))).
		Else(clientv3.OpGet("/lock/active"))
	txnResp, err := txn.Commit()
	if err != nil {
		logrus.Errorf("failed to commit a etcd transaction: %v", err)
	}
	if !txnResp.Succeeded {
		_, err = lease.Revoke(context.TODO(), leaseResp.ID)
		return false, string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value), err
	}
	return true, etcd.hostIP, nil
}

//ActiveKillItself cancels the renew of lease.
func (etcd *EtcdMgr) StopKeepHeartBeat(mark string) bool {
	var err error
	if mark == "active" {
		_, err = etcd.client.Revoke(context.TODO(), etcd.activeLeaseResp.ID)
	} else {
		_, err = etcd.client.Revoke(context.TODO(), etcd.standbyLeaseResp.ID)
	}
	if err != nil {
		logrus.Errorf("failed to cancel a etcd lease: %v", err)
		return false
	}
	logrus.Info("success to cancel a etcd lease")
	return true

}

//CloseTool close the tool used to implement supernode ha.
func (etcd *EtcdMgr) CloseTool() error {
	var err error
	if err = etcd.client.Close(); err != nil {
		logrus.Info("success to close a etcd client")
		return nil
	}
	return err
}

func (etcd *EtcdMgr) SendStandbySupernodesInfo(key string, value string, timeout int64) error {
	kv := clientv3.NewKV(etcd.client)
	lease := clientv3.NewLease(etcd.client)
	leaseResp, e := lease.Grant(context.TODO(), timeout)
	if _, e = kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID)); e != nil {
		return e
	}
	etcd.standbyLeaseResp = leaseResp
	respchan, e := lease.KeepAlive(context.TODO(), leaseResp.ID)
	if e != nil {
		return e
	}
	go func() {
		for {
			<-respchan
		}
	}()
	return nil
}
func (etcd *EtcdMgr) GetStandbySupenrodesInfo(key string) ([]string, error) {
	kv := clientv3.NewKV(etcd.client)
	getRes, e := kv.Get(context.TODO(), key, clientv3.WithPrefix())
	if e != nil {
		return nil, e
	}
	var values []string
	for _, v := range getRes.Kvs {
		values = append(values, string(v.Value))

	}
	fmt.Println("value", values)
	return values, nil
}

//WatchActiveChange is the progress to watch the etcd,if the value of key /lock/active changes,supernode will be notified.
func (etcd *EtcdMgr) WatchStandbySupernodes(key string, standby *[]string) {
	var watchStartRevision int64
	watcher := clientv3.NewWatcher(etcd.client)
	watchChan := watcher.Watch(context.TODO(), key, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			fmt.Printf("standby supernodes have changed(0:add 1:delete)code: %d\n", int(event.Type))
			*standby, _ = etcd.GetStandbySupenrodesInfo(key)
		}
	}
}
