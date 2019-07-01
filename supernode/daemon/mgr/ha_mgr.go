package mgr


type HaMgr interface {
	//HaElection try to use etcd\zookeeper...to obtain the lock and become the active supernode
	HaElect()
	//

}
