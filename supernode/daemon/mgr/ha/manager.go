package ha

import (
	"context"
	"fmt"
	apiTypes "github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"math/rand"
	"os"
	"time"

	"github.com/go-openapi/strfmt"
	//"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Manager is the struct to manager supernode ha.
type Manager struct {
	nodeStatus int
	tool       Tool
	config     *config.Config

	PeerMgr      mgr.PeerMgr
	DfgetTaskMgr mgr.DfgetTaskMgr
	ProgressMgr  mgr.ProgressMgr
	CDNMgr       mgr.CDNMgr
	SchedulerMgr mgr.SchedulerMgr
	HTTPClient   httputils.SimpleHTTPClient
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
		config:     cfg,
		HTTPClient: httputils.DefaultHTTPClient,

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

func (ha *Manager) SendPostCopy(ctx context.Context, req interface{}, path string, node *config.SupernodeInfo) error {
	url := fmt.Sprintf("%s://%s:%d%s", "http", node.IP, node.ListenPort, path)
	if _, _, e := ha.Post(url, req, 5*time.Second); e != nil {
		logrus.Errorf("failed to send post copy,err: %v", e)
		return e
	}
	return nil
}

//Post sends post request to supernode
func (ha *Manager) Post(url string, body interface{}, timeout time.Duration) (code int, res []byte, e error) {
	return ha.HTTPClient.PostJSON(url, body, 5*time.Second)
}

// SendRegisterRequestCopy send dfget register req copy to other supernode to register
func (ha *Manager) TriggerOtherSupernodeDownload(ctx context.Context, req *apiTypes.TaskRegisterRequest) error {
	index := ha.randomSelectSupernodeTriggerCDN(ctx)
	if index == -1 {
		return nil
	}
	err := ha.config.GetOtherSupernodeInfo()[index].RPCClient.Call("RpcManager.RpcOnlyTriggerCDNDownload", req, nil)
	if err != nil {
		logrus.Errorf("failed to trigger CDN download via rpc,err: %v", err)
		return err
	}
	return nil
}

func (ha *Manager) randomSelectSupernodeTriggerCDN(ctx context.Context) int {
	if supernodeNum := len(ha.config.OtherSupernodes); supernodeNum == 0 {
		return -1
	}
	return rand.Intn(len(ha.config.OtherSupernodes))
}
