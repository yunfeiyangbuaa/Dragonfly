package ha

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	apiTypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Manager is the struct to manager supernode ha.
type Manager struct {
	nodeStatus int
	tool       Tool
	copyAPI    api.SupernodeAPI
	config     *config.Config

	PeerMgr      mgr.PeerMgr
	DfgetTaskMgr mgr.DfgetTaskMgr
	ProgressMgr  mgr.ProgressMgr
	CDNMgr       mgr.CDNMgr
	SchedulerMgr mgr.SchedulerMgr
}

// NewManager produces the Manager object.
func NewManager(cfg *config.Config, peerMgr mgr.PeerMgr, dfgetTaskMgr mgr.DfgetTaskMgr, progressMgr mgr.ProgressMgr, cDNMgr mgr.CDNMgr, schedulerMgr mgr.SchedulerMgr) (*Manager, error) {
	var (
		toolMgr Tool
		err     error
	)
	if cfg.UseHA != false {
		toolMgr, err = NewEtcdMgr(cfg, peerMgr, progressMgr)
		if err != nil {
			logrus.Errorf("failed to init the ha tool: %v", err)
			return nil, err
		}
	}
	return &Manager{
		config:  cfg,
		copyAPI: api.NewSupernodeAPI(),

		tool:         toolMgr,
		DfgetTaskMgr: dfgetTaskMgr,
		PeerMgr:      peerMgr,
		CDNMgr:       cDNMgr,
		ProgressMgr:  progressMgr,
		SchedulerMgr: schedulerMgr,
	}, nil
}

// HADaemon is the main progress to implement active/standby switch.
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
func (ha *Manager) CloseHaManager(ctx context.Context) error {
	return ha.tool.Close(ctx)
}

// SendGetCopy sends dfget's get request copy to standby supernode
func (ha *Manager) SendGetCopy(ctx context.Context, path string, params string, node config.SupernodeInfo) error {
	urlCopy := fmt.Sprintf("%s://%s:%d%s?%s",
		"http", node.IP, node.ListenPort, path, params)
	resp := new(types.BaseResponse)
	if e := ha.copyAPI.Get(urlCopy, resp); e != nil {
		logrus.Errorf("failed to send %s get copy,err: %v", path, e)
		return e
	}
	return nil
}

// SendPostCopy sends dfget's post request copy to standby supernode
func (ha *Manager) SendPostCopy(ctx context.Context, req interface{}, path string, node config.SupernodeInfo) error {
	url := fmt.Sprintf("%s://%s:%d%s", "http", node.IP, node.ListenPort, path)
	if _, _, e := ha.copyAPI.Post(url, req, 5*time.Second); e != nil {
		logrus.Errorf("failed to send post copy,err: %v", e)
		return e
	}
	return nil
}

// SendRegisterRequestCopy send dfget register req copy to other supernode to register
func (ha *Manager) SendRegisterRequestCopy(ctx context.Context, req *apiTypes.TaskRegisterRequest, triggerDownload bool) {
	index := ha.randomSelectSupernodeTriggerCDN(ctx)
	if index == -1 {
		return
	}
	req.TriggerCDN = constants.TriggerFalse
	for i, node := range ha.config.OtherSupernodes {
		if i == index && triggerDownload == true {
			continue
		}
		if err := ha.SendPostCopy(ctx, req, "/peer/registry", node); err != nil {
			logrus.Errorf("failed to send register copy to %s,err: %v", node.PID, err)
		}
	}
	if triggerDownload == true {
		req.TriggerCDN = constants.TriggerBySupernode
		if err := ha.SendPostCopy(ctx, req, "/peer/registry", ha.config.OtherSupernodes[index]); err != nil {
			logrus.Errorf("failed to send register copy to %s,err: %v", ha.config.OtherSupernodes[index].PID, err)
		}
	}
}

func (ha *Manager) randomSelectSupernodeTriggerCDN(ctx context.Context) int {
	if supernodeNum := len(ha.config.OtherSupernodes); supernodeNum == 0 {
		return -1
	}
	return rand.Intn(len(ha.config.OtherSupernodes))
}

func (ha *Manager) addSupernodeCdnResource(ctx context.Context, task *apiTypes.TaskInfo, node config.SupernodeInfo) error {
	if node.PID == ha.config.GetSuperPID() {
		return nil
	}
	cid := fmt.Sprintf("%s:%s~%s", "cdnnode", node.IP, task.ID)
	path, err := ha.CDNMgr.GetHTTPPath(ctx, task.ID)
	if err != nil {
		return err
	}
	if err := ha.DfgetTaskMgr.Add(ctx, &apiTypes.DfGetTask{
		CID:       cid,
		Path:      path,
		PeerID:    node.PID,
		PieceSize: task.PieceSize,
		Status:    apiTypes.DfGetTaskStatusWAITING,
		TaskID:    task.ID,
	}); err != nil {
		return errors.Wrapf(err, "failed to add cdn dfgetTask for taskID %s", task.ID)
	}
	ha.ProgressMgr.InitProgress(ctx, task.ID, node.PID, cid)
	return nil
}
