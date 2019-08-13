package ha

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/go-openapi/strfmt"
	"math/rand"
	"time"

	"net/rpc"
	"os"

	apiTypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/supernode/config"

	"github.com/sirupsen/logrus"
)

// Manager is the struct to manager supernode ha.
type Manager struct {
	advertiseIP  string
	useHa        bool
	nodeStatus   int
	tool         Tool
	copyAPI      api.SupernodeAPI
	config       *config.Config
	rpcClient    *rpc.Client
	PeerMgr      mgr.PeerMgr
	DfgetTaskMgr mgr.DfgetTaskMgr
	ProgressMgr  mgr.ProgressMgr
	CDNMgr       mgr.CDNMgr
	SchedulerMgr mgr.SchedulerMgr
}

// NewManager produces the Manager object.
func NewManager(cfg *config.Config, peerMgr mgr.PeerMgr, dfgetTaskMgr mgr.DfgetTaskMgr, progressMgr mgr.ProgressMgr, cDNMgr mgr.CDNMgr, schedulerMgr mgr.SchedulerMgr) (*Manager, error) {
	//TODO(yunfeiyangbuaa): handle the NewEtcdMgr(cfg) in the future
	var (
		toolMgr Tool
		err     error
	)
	if cfg.UseHA != false {
		toolMgr, err = NewEtcdMgr(cfg, peerMgr)
		if err != nil {
			logrus.Errorf("failed to init the ha tool: %v", err)
			return nil, err
		}
	}
	return &Manager{
		advertiseIP:  cfg.AdvertiseIP,
		useHa:        cfg.UseHA,
		tool:         toolMgr,
		copyAPI:      api.NewSupernodeAPI(),
		config:       cfg,
		DfgetTaskMgr: dfgetTaskMgr,
		PeerMgr:      peerMgr,
		CDNMgr:       cDNMgr,
		ProgressMgr:  progressMgr,
		SchedulerMgr: schedulerMgr,
	}, nil
}

// ElectDaemon is the main progress to implement active/standby switch.
func (ha *Manager) HADaemon(ctx context.Context) error {
	hostname, _ := os.Hostname()
	pid := ha.config.GetSuperPID()
	standbyAddress := fmt.Sprintf("%s%s:%d", supernodeKeyPrefix, ha.config.AdvertiseIP, ha.config.ListenPort)
	if err := ha.tool.SendSupernodesInfo(ctx, standbyAddress, ha.config.AdvertiseIP, pid, ha.config.ListenPort, ha.config.DownloadPort, ha.config.HARpcPort, strfmt.Hostname(hostname), 2); err != nil {
		logrus.Errorf("failed to send supernode info to other supernode,err %v", err)
		return err
	}
	// a process to watch the standby supernode's status.
	go ha.tool.WatchSupernodesChange(ctx, supernodeKeyPrefix)
	return nil
}

// CloseHaManager closes the tool use to implement supernode ha.
func (ha *Manager) CloseHaManager() error {
	return ha.tool.Close(context.TODO())
}

// SendGetCopy sends dfget's get request copy to standby supernode
func (ha *Manager) SendGetCopy(path string, node config.SupernodeInfo) error {
	urlCopy := fmt.Sprintf("%s://%s:%d%s", "http", node.IP, node.ListenPort, path)
	resp := new(types.BaseResponse)
	e := ha.copyAPI.Get(urlCopy, resp)
	if e != nil {
		logrus.Errorf("failed to send %s get copy,err: %v", path, e)
		return e
	}
	return nil
}

// SendPostCopy sends dfget's post request copy to standby supernode
func (ha *Manager) SendPostCopy(req interface{}, path string, node config.SupernodeInfo) error {
	url := fmt.Sprintf("%s://%s:%d%s", "http", node.IP, node.ListenPort, path)
	_, _, e := ha.copyAPI.Post(url, req, 5*time.Second)
	if e != nil {
		logrus.Errorf("failed to send post copy,err: %v", e)
		return e
	}
	return nil
}

//func (ha *Manager) AddOtherSupernodesCdnResource(task *apiTypes.TaskInfo) {
//	for _, node := range ha.config.OtherSupernodes {
//		ha.AddSupernodeCdnResource(task, node)
//	}
//}types.TaskRegisterRequest
func (ha *Manager) SendRegisterRequestCopy(req *apiTypes.TaskRegisterRequest, triggerDownload bool) {
	index := ha.randomSelectSupernodeTriggerCDN()
	if index == -1 {
		return
	}
	req.PeerID = req.PeerID
	req.TriggerCDN = constants.TriggerFalse
	for i, node := range ha.config.OtherSupernodes {
		if i == index && triggerDownload == true {
			continue
		}
		if err := ha.SendPostCopy(req, "/peer/registry", node); err != nil {
			logrus.Errorf("failed to send register copy to %s,err: %v", node.PID, err)
		}
	}
	if triggerDownload == true {
		req.TriggerCDN = constants.TriggerBySupernode
		if err := ha.SendPostCopy(req, "/peer/registry", ha.config.OtherSupernodes[index]); err != nil {
			logrus.Errorf("failed to send register copy to %s,err: %v", ha.config.OtherSupernodes[index].PID, err)
		}
	}
}

func (ha *Manager) randomSelectSupernodeTriggerCDN() int {
	supernodeNum := len(ha.config.OtherSupernodes)
	if supernodeNum == 0 {
		return -1
	}
	return rand.Intn(len(ha.config.OtherSupernodes))
}
