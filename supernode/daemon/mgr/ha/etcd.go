package ha


import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"context"
	"log"
	"fmt"
)


type Tool interface{
	TryBeActive(ctx context.Context)(bool,error)
	WatchActiveChange()
}
type toolMgr  struct{
	config clientv3.Config
	client *clientv3.Client
	kv clientv3.KV
	leaseTtl int64
	leasettl int64
	ip  string//i  will try to use a already existing config struct to  replace it
	leaseResp *clientv3.LeaseGrantResponse

}
func NewToolMgr(haconf haConfig)(*toolMgr,error){
	config:= clientv3.Config{
		Endpoints:[]string{haconf.port},
		DialTimeout: haconf.timeout*time.Second,
	}
	// build connection to etcd
	client,error:= clientv3.New(config)
	return &toolMgr{
		config:config,
		client:client,
	},error
}
func (etcd *toolMgr)WathActiveChange(ctx context.Context)(bool,error){






}



func (etcd *toolMgr)TryBeActive(ctx context.Context)(bool,error){
	//用于读写etcd 的键值对
	kv := clientv3.NewKV(etcd.client)
	// 1 上锁（创建租约，自动续租，拿着租约去抢占一个key）
	lease := clientv3.NewLease(etcd.client)
	leaseResp,err := lease.Grant(context.TODO(),etcd.leasettl)
	_, kaerr := lease.KeepAlive(context.TODO(), leaseResp.ID)
	if kaerr != nil {
		log.Fatal(kaerr)
	}
	// if 不存在key，then
	txn:= kv.Txn(context.TODO())
	txn.If(clientv3.Compare(clientv3.CreateRevision("/lock/active"),"=",0)).
		Then(clientv3.OpPut("/lock/actyive",etcd.ip,clientv3.WithLease(leaseResp.ID))).
		Else(clientv3.OpGet("/lock/active"))   //i will deal with it
	txnResp,err := txn.Commit()
	if err != nil{
		log.Fatal(err)
	}
	if !txnResp.Succeeded{
		log.Fatal("锁被占用：",string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
		return false,nil
	}else {
		fmt.Println("锁未被占用：")
		return true,nil
	}
}















