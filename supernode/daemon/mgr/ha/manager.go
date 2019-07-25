package ha

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly/common/constants"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

//Manager is the struct to manager supernode ha.
type Manager struct {
	advertiseIP       string
	useHa             bool
	nodeStatus        int
	tool              Tool
	copyAPI           api.SupernodeAPI
	standbySupernodes []string
	config            *config.Config
}

//NewManager produce the Manager object.
func NewManager(cfg *config.Config) (*Manager, error) {
	//TODO(yunfeiyangbuaa): handle the NewEtcdMgr(cfg) in the future
	toolMgr, err := NewEtcdMgr(cfg)
	if err != nil {
		logrus.Errorf("failed to initial the ha tool: %v", err)
		return nil, err
	}
	return &Manager{
		advertiseIP: cfg.AdvertiseIP,
		useHa:       cfg.UseHA,
		nodeStatus:  constants.SupernodeUseHaInit,
		tool:        toolMgr,
		copyAPI:     api.NewSupernodeAPI(),
		config:      cfg,
	}, nil
}

//ElectDaemon is the main progress to implement active/standby switch.
func (ha *Manager) ElectDaemon(change chan int) {
	messageChannel := make(chan string)
	//ha.GetStandbySupernodeInfo("/supernodes/standby/")
	//a process to watch whether the active supernode is off.
	go ha.watchActive(messageChannel)
	//a process try to get the active supernode when the supernode is start.
	go ha.tryStandbyToActive(change)
	for {
		if activeIP, ok := <-messageChannel; ok {
			//when the active node is off.
			if activeIP == ActiveSupernodeOFF {
				//if the previous active supernode is itself,change its status to standby to avoid brain split.
				if ha.nodeStatus == constants.SupernodeUseHaActive {
					ha.activeToStandby()
					change <- constants.SupernodeUsehakill
				} else {
					ha.tryStandbyToActive(change)
				}
			}
		}
	}

}

//GetSupernodeStatus get supernode's status.
func (ha *Manager) GetSupernodeStatus() int {
	if ha.useHa == false {
		return constants.SupernodeUseHaFalse
	}
	return ha.nodeStatus
}

//CompareAndSetSupernodeStatus set supernode's status.
func (ha *Manager) CompareAndSetSupernodeStatus(preStatus int, nowStatus int) bool {
	if ha.nodeStatus == preStatus {
		ha.nodeStatus = nowStatus
		return true
	}
	logrus.Errorf("failed to set supernode status,the preStatus is %d not equal to %d", ha.nodeStatus, preStatus)
	return false
}

//CloseHaManager close the tool use to implement supernode ha.
func (ha *Manager) CloseHaManager() error {
	return ha.tool.CloseTool()
}

//GiveUpActiveStatus give up its active status because of unhealthy.
func (ha *Manager) StopSendHeartBeat(mark string) bool {
	return ha.tool.StopKeepHeartBeat(mark)
}

//SendGetCopy send dfget's get request copy to standby supernode
func (ha *Manager) SendGetCopy(params string) {
	for _, node := range ha.standbySupernodes {
		fmt.Println("++++++++++send get copy to+++++++++++++++++++++++++++++++++++++++++++++++", node)
		urlCopy := fmt.Sprintf("%s://%s%s", "http", node, params)
		e := ha.copyAPI.Get(urlCopy, "")
		if e != nil {
			logrus.Errorf("failed to send get copy,err: %v", e)
		}
	}
}

//SendPostCopy send dfget's post request copy to standby supernode
func (ha *Manager) SendPostCopy(req interface{}, path string) {
	fmt.Println("#############################", ha.standbySupernodes)
	for _, node := range ha.standbySupernodes {
		fmt.Println("++++++++++send post copy to+++++++++++++++++++++++++++++++++++++++++++++++", node)
		url := fmt.Sprintf("%s://%s%s", "http", node, path)
		code, _, e := ha.copyAPI.Post(url, req, 5*time.Second)
		if e != nil {
			logrus.Errorf("failed to send post copy,err: %v", e)
		}
		if code != 200 {
			logrus.Errorf("failed to send post copy,err %v,code %d not equal to 200", e, code)
		}
	}
}

//StandbyToActive change the status from standby to active.
func (ha *Manager) standbyToActive() {
	if ha.nodeStatus == constants.SupernodeUseHaStandby {
		ha.StopSendHeartBeat("standby")
	}
	go ha.WatchStandbySupernode("/supernodes/standby/")
	if ha.nodeStatus == constants.SupernodeUseHaStandby || ha.nodeStatus == constants.SupernodeUseHaInit {
		ha.nodeStatus = constants.SupernodeUseHaActive
	} else {
		logrus.Warnf("%s is already active,can't set it active again", ha.advertiseIP)
	}
}

//ActiveToStandby  change the status from active to standby.
func (ha *Manager) activeToStandby() {
	if ha.nodeStatus == constants.SupernodeUseHaInit {
		ha.nodeStatus = constants.SupernodeUseHaStandby
		ha.StoreStandbySupernodeInfo()
	} else if ha.nodeStatus == constants.SupernodeUseHaActive {
		ha.nodeStatus = constants.SupernodeUseHaStandby
	} else {
		logrus.Warnf("%s is already standby,can't set it standby again", ha.advertiseIP)
	}
}

//TryStandbyToActive try to change the status from standby to active.
func (ha *Manager) tryStandbyToActive(change chan int) {
	is, ip, err := ha.tool.TryBeActive()
	if err != nil {
		logrus.Errorf("failed to try to change standby status to active status")
	}
	if is == true {
		ha.standbyToActive()
		logrus.Infof("%s obtain the active supernode status", ha.advertiseIP)
		change <- constants.SupernodeUseHaActive
		ha.tool.ActiveResureItsStatus()
		ha.activeToStandby()
		logrus.Infof("%s finishes the active supernode status", ha.advertiseIP)
	} else {
		ha.activeToStandby()
		logrus.Infof("the other supernode %s obtain the active supernode status,keep watch on it", ip)
		change <- constants.SupernodeUseHaStandby
	}
}

//WatchActive keep watch whether the active supernode is off.
func (ha *Manager) watchActive(messageChannel chan string) {
	ha.tool.WatchActiveChange(messageChannel)
}

func (ha *Manager) StoreStandbySupernodeInfo() error {
	fmt.Println("send standby info to etcd")
	if e := ha.tool.SendStandbySupernodesInfo("/supernodes/standby/"+ha.advertiseIP, ha.advertiseIP+":"+strconv.Itoa(ha.config.HAStandbyPort), 1); e != nil {
		logrus.Error("failed to send standby Info to active supernode")
		return e
	}
	return nil
}

func (ha *Manager) GetStandbySupernodeInfo(keyPreFIx string) error {
	fmt.Println("get standby info from etcd")
	nodes, e := ha.tool.GetStandbySupenrodesInfo(keyPreFIx)
	if e != nil {
		logrus.Error("failed to get standby supernodes info")
		return e
	}
	ha.standbySupernodes = nodes
	fmt.Println("standby supernodeinfo:", ha.standbySupernodes)
	return nil
}

//WatchActiveChange is the progress to watch the etcd,if the value of key /lock/active changes,supernode will be notified.
func (ha *Manager) WatchStandbySupernode(keyPreFIx string) {
	ha.GetStandbySupernodeInfo(keyPreFIx)
	go ha.tool.WatchStandbySupernodes(keyPreFIx, &ha.standbySupernodes)
	//go func(){
	//	for{
	//		time.Sleep(3*time.Second)
	//		fmt.Println("standby supernode:",ha.standbySupernodes)
	//	}
	//}()
}
