package mgr

//HaMgr is the interface to implement supernode Ha.
type HaMgr interface {
	//ElectDaemonthe is the daemon progress to implement active/standby switch.
	ElectDaemon(change chan int)

	//HagetSupernodeState get supernode's status.
	GetSupernodeStatus() int

	//HaSetSupernodeState compare and set supernode's status.
	CompareAndSetSupernodeStatus(preStatus int, nowStatus int) bool

	//CloseHaManager close the tool use to implement supernode ha
	CloseHaManager() error

	//GiveUpActiveStatus give up its active status because of unhealthy
	GiveUpActiveStatus() bool
}
