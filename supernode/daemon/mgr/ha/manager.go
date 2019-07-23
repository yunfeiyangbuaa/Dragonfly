package ha

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly/common/constants"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"time"

	"github.com/sirupsen/logrus"
)

//Manager is the struct to manager supernode ha.
type Manager struct {
	advertiseIP string
	useHa       bool
	nodeStatus  int
	tool        Tool
	copyAPI     api.SupernodeAPI
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
	}, nil
}

//ElectDaemon is the main progress to implement active/standby switch.
func (ha *Manager) ElectDaemon(change chan int) {
	messageChannel := make(chan string)
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
func (ha *Manager) GiveUpActiveStatus() bool {
	return ha.tool.ActiveKillItself()
}

//SendGetCopy send dfget's get request copy to standby supernode
func (ha *Manager) SendGetCopy(params string, node string) error {
	urlCopy := fmt.Sprintf("%s://%s%s", "http", node, params)
	//resp := new(dfType.BaseResponse)
	e := ha.copyAPI.Get(urlCopy, "")
	if e != nil {
		logrus.Errorf("failed to send get copy,err: %v", e)
	}
	return e
}

//SendPostCopy send dfget's post request copy to standby supernode
func (ha *Manager) SendPostCopy(req interface{}, node string, path string) ([]byte,error){
	url := fmt.Sprintf("%s://%s%s", "http", node, path)
	code, resp, e := ha.copyAPI.Post(url, req, 5*time.Second)
	if e != nil {
		logrus.Errorf("failed to send post copy,err: %v", e)
		return nil,e
	} else if code != 200 {
		logrus.Errorf("failed to send post copy,err %v,code %d not equal to 200", e, code)
		return nil,e
	}
	return resp,nil
}

//StandbyToActive change the status from standby to active.
func (ha *Manager) standbyToActive() {
	if ha.nodeStatus == constants.SupernodeUseHaStandby {
		ha.nodeStatus = constants.SupernodeUseHaActive
	} else {
		logrus.Warnf("%s is already active,can't set it active again", ha.advertiseIP)
	}
}

//ActiveToStandby  change the status from active to standby.
func (ha *Manager) activeToStandby() {
	if ha.nodeStatus == constants.SupernodeUseHaActive {
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
		logrus.Infof("the other supernode %s obtain the active supernode status,keep watch on it", ip)
		change <- constants.SupernodeUseHaStandby
	}
}

//WatchActive keep watch whether the active supernode is off.
func (ha *Manager) watchActive(messageChannel chan string) {
	ha.tool.WatchActiveChange(messageChannel)
}
