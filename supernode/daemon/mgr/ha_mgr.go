package mgr

//HaMgr is the interface to implement supernode Ha.
type HaMgr interface {
	//ElectDaemonthe is the daemon progress to implement active/standby switch.
	ElectDaemon(change chan int)

	//HagetSupernodeState get supernode's status.
	GetSupernodeStatus() int

	//HaSetSupernodeState compare and set supernode's status.
	CompareAndSetSupernodeStatus(preStatus int, nowStatus int) bool

	//CloseHaManager close the tool used to implement supernode ha.
	CloseHaManager() error

	//GiveUpActiveStatus give up its active status because of unhealthy.
	GiveUpActiveStatus() bool

	//SendGetCopy send dfget's get request copy to standby supernode
	SendGetCopy(params string, node string) error

	//SendPostCopy send dfget's post request copy to standby supernode
	SendPostCopy(req interface{}, node string, path string)([]byte,error)
}
