package ha

import (
	"github.com/dragonflyoss/Dragonfly/common/constants"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/sirupsen/logrus"
)

//Manager is the struct to manager supernode ha.
type Manager struct {
	advertiseIP string
	useHa       bool
	nodeStatus  int
	tool        Tool
}

//NewManager produce the Manager object.
func NewManager(cfg *config.Config) (*Manager, error) {
	toolMgr, err := NewEtcdMgr(cfg)
	if err != nil {
		logrus.Errorf("failed to intial the ha tool: %v", err)
		return nil, err
	}
	return &Manager{
		advertiseIP: cfg.AdvertiseIP,
		useHa:       cfg.UseHA,
		nodeStatus:  constants.SupernodeUseHaInit,
		tool:        toolMgr,
	}, nil
}

//TODO(yunfeiyangbuaa): handle these log,errors and exceptions in the future

//ElectDaemon is the main progress to implement active/standby switch.
func (ha *Manager) ElectDaemon(change chan int) {
	messageChannel := make(chan string)
	//a process to watch whether the active supernode is off
	go ha.watchActive(messageChannel)
	//a process try to get the active supernode when the supernode is start
	go ha.tryStandbyToActive(change)
	for {
		if activeIP, ok := <-messageChannel; ok {
			//the active node is off.
			if activeIP == ActiveSupernodeOFF {
				//if the previous active supernode is itself.
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

//CloseHaManager close the tool use to implement supernode ha
func (ha *Manager) CloseHaManager() error {
	return ha.tool.CloseTool()
}

//GiveUpActiveStatus give up its active status because of unhealthy
func (ha *Manager) GiveUpActiveStatus() bool {
	return ha.tool.ActiveKillItself()
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
	finished := make(chan bool, 10)
	is, ip, err := ha.tool.TryBeActive(finished)
	if err != nil {
		logrus.Errorf("failed to try to change standby status to active status")
	}
	if is == true {
		ha.standbyToActive()
		logrus.Infof("%s obtain the active supernode status", ha.advertiseIP)
		change <- constants.SupernodeUseHaActive
		go ha.tool.ActiveResureItsStatus(finished)
		<-finished
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
