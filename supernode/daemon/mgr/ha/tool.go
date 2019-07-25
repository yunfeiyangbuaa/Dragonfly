package ha

//Tool is an interface that use etcd/zookeeper/yourImplement tools to make supernode be standby or active.
type Tool interface {
	//WatchActiveChange keeps watching the status of active supernode.
	WatchActiveChange(messageChannel chan string)

	//ObtainActiveInfo obtains the active supernode's info(Ip address and port).
	ObtainActiveInfo(key string) (string, error)

	//TryBeActive try to make standby supernode to be active.
	TryBeActive() (bool, string, error)

	//ActiveResureItsStatus will keep to monitor to ensure this itself is still a active supernode now.
	ActiveResureItsStatus()

	//ActiveKillItself abandon the active status and the active supernode become standby supernode.
	StopKeepHeartBeat(mark string) bool

	//CloseTool close the tool.
	CloseTool() error

	SendStandbySupernodesInfo(key string, value string, timeout int64) error

	GetStandbySupenrodesInfo(key string) ([]string, error)

	WatchStandbySupernodes(key string, standby *[]string)
}
