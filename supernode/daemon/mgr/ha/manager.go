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

//func (ha *Manager) addSupernodeCdnResource(ctx context.Context, task *apiTypes.TaskInfo, node config.SupernodeInfo) error {
//	if node.PID == ha.config.GetSuperPID() {
//		return nil
//	}
//	cid := fmt.Sprintf("%s:%s~%s", "cdnnode", node.IP, task.ID)
//	path, err := ha.CDNMgr.GetHTTPPath(ctx, task.ID)
//	if err != nil {
//		return err
//	}
//	if err := ha.DfgetTaskMgr.Add(ctx, &apiTypes.DfGetTask{
//		CID:       cid,
//		Path:      path,
//		PeerID:    node.PID,
//		PieceSize: task.PieceSize,
//		Status:    apiTypes.DfGetTaskStatusWAITING,
//		TaskID:    task.ID,
//	}); err != nil {
//		return errors.Wrapf(err, "failed to add cdn dfgetTask for taskID %s", task.ID)
//	}
//	ha.ProgressMgr.InitProgress(ctx, task.ID, node.PID, cid)
//	return nil
//}

//// SendGetCopy sends dfget's get request copy to standby supernode
//func (ha *Manager) SendGetCopy(ctx context.Context, path string, params string, node config.SupernodeInfo) error {
//	urlCopy := fmt.Sprintf("%s://%s:%d%s?%s",
//		"http", node.IP, node.ListenPort, path, params)
//	resp := new(types.BaseResponse)
//	if e := ha.copyAPI.Get(urlCopy, resp); e != nil {
//		logrus.Errorf("failed to send %s get copy,err: %v", path, e)
//		return e
//	}
//	return nil
//}
//
//// SendPostCopy sends dfget's post request copy to standby supernode
//func (ha *Manager) SendPostCopy(ctx context.Context, req interface{}, path string, node config.SupernodeInfo) error {
//	url := fmt.Sprintf("%s://%s:%d%s", "http", node.IP, node.ListenPort, path)
//	if _, _, e := ha.copyAPI.Post(url, req, 5*time.Second); e != nil {
//		logrus.Errorf("failed to send post copy,err: %v", e)
//		return e
//	}
//	return nil
//}

//func (rpc *RpcManager) RpcReportCache(taskID string, resp *RpcReportCacheRequest) error {
//	task, err := rpc.TaskMgr.Get(context.TODO(), taskID)
//	if err != nil {
//		return err
//	}
//	startNum, hash, _, err := rpc.CdnMgr.DetectCacheForHA(context.TODO(), task)
//	if err != nil {
//		return err
//	}
//
//	resp = &RpcReportCacheRequest{
//		//CdnStatus:  updateTask.CdnStatus,
//		//FileLength: updateTask.FileLength,
//		//RealMd5:    updateTask.RealMd5,
//		StartNum: startNum,
//		Hash:     hash,
//	}
//	return nil
//}
//func (rpc *RpcManager) RpcIntCdn(req InitCdnRequest, resp *bool) error {
//	ctx := context.TODO()
//	if req.NodePID == rpc.cfg.GetSuperPID() {
//		return nil
//	}
//	cid := fmt.Sprintf("%s:%s~%s", "cdnnode", req.NodeIP, req.TaskID)
//	path, err := rpc.CdnMgr.GetHTTPPath(ctx, req.TaskID)
//	if err != nil {
//		return err
//	}
//	if err := rpc.DfgetTaskMgr.Add(context.Background(), &types.DfGetTask{
//		CID:       cid,
//		Path:      path,
//		PeerID:    req.NodePID,
//		PieceSize: req.TaskPieceSize,
//		Status:    types.DfGetTaskStatusWAITING,
//		TaskID:    req.TaskID,
//	}); err != nil {
//		return errors.Wrapf(err, "failed to add cdn dfgetTask for taskID %s", req.TaskID)
//	}
//	rpc.ProgressMgr.InitProgress(context.Background(), req.TaskID, req.NodePID, cid)
//	return nil
//}
//
////func (rpc *RpcManager) RpcReportPiece(req ReportPieceRequest, resp *bool) error {
////	return rpc.CdnMgr.ReportPieceStatusForHA(context.TODO(), req.TaskID, req.CID, req.SrcPID, req.PieceNum, req.Md5, req.PieceStatus)
////}
//
//func (rpc *RpcManager) RpcUpdateTaskInfo(req RpcUpdateTaskInfoRequest, resp *bool) error {
//	var (
//		updateTask *types.TaskInfo
//		err        error
//	)
//	if req.CdnStatus == types.TaskInfoCdnStatusFAILED {
//		updateTask = &types.TaskInfo{
//			CdnStatus: types.DfGetTaskStatusFAILED,
//		}
//	} else if req.CdnStatus == types.TaskInfoCdnStatusSUCCESS {
//		updateTask = &types.TaskInfo{
//			CdnStatus:  req.CdnStatus,
//			FileLength: req.FileLength,
//			RealMd5:    req.RealMd5,
//		}
//	} else {
//		return errors.Wrapf(err, "failed to update taskinfo via rpc,unexpected req: %v", req)
//	}
//	if err = rpc.TaskMgr.Update(context.TODO(), req.TaskID, updateTask); err != nil {
//		logrus.Errorf("failed to update task %v via rpc,err: %v", updateTask, req)
//		return err
//	}
//	logrus.Infof("success to update task cdn via rpc %+v", updateTask)
//	return nil
//}
//
//func (rpc *RpcManager) RpcUpdateDfgetTask(req RpcUpdateDfgetRequest, resp *bool) error {
//	return rpc.DfgetTaskMgr.UpdateStatus(context.TODO(), req.ClientID, req.TaskID, req.DfgetTaskStatus)
//}
//
//func (rpc *RpcManager) RpcUpdateClientProgress(req RpcUpdateClientProgressRequest, resp *bool) error {
//	return rpc.ProgressMgr.UpdateClientProgress(context.TODO(), req.TaskID, req.ClientID, req.DstPID, req.PieceNum, req.PieceStatus)
//}
//
//func (rpc *RpcManager) RpcPeerLoadAddOne(peerID string, resp *bool) error {
//	peerState, err := rpc.ProgressMgr.GetPeerStateByPeerID(context.TODO(), peerID)
//	if err != nil {
//		return err
//	}
//	if peerState.ProducerLoad != nil {
//		if peerState.ProducerLoad.Add(1) <= config.PeerUpLimit {
//			return nil
//		}
//		peerState.ProducerLoad.Add(-1)
//	}
//	return nil
//}
//type RpcReportCacheRequest struct {
//	CdnStatus  string
//	FileLength int64
//	RealMd5    string
//	StartNum   int
//	Hash       hash.Hash
//}

////clientID, taskID, dfgetTaskStatus
//type RpcUpdateDfgetRequest struct {
//	ClientID        string
//	TaskID          string
//	DfgetTaskStatus string
//}

////ctx, taskID, clientID, dstPID, pieceNums[i], config.PieceRUNNING
//type RpcUpdateClientProgressRequest struct {
//	TaskID      string
//	ClientID    string
//	DstPID      string
//	PieceNum    int
//	PieceStatus int
//}

//type InitCdnRequest struct {
//	TaskID        string
//	TaskPieceSize int32
//	NodePID       string
//	NodeIP        string
//}

//// registerOtherSupernodesAsPeer registers all other supernodes as a peer
//func (etcd *EtcdMgr) registerOtherSupernodesAsPeer(ctx context.Context) {
//	supernodes := etcd.config.GetOtherSupernodeInfo()
//	for _, node := range etcd.config.GetOtherSupernodeInfo() {
//		if err := etcd.registerSupernodeAsPeer(ctx, node); err != nil {
//
//			// if failed,delete the supernode in config
//			var newSupernodes []config.SupernodeInfo
//			for k, v := range supernodes {
//				if v == node {
//					kk := k + 1
//					newSupernodes = append(supernodes[:k], supernodes[kk:]...)
//				}
//			}
//			etcd.config.SetOtherSupernodeInfo(newSupernodes)
//		}
//	}
//
//}
//
//// registerSupernodeAsPeer registers supernode as a peer
//func (etcd *EtcdMgr) registerSupernodeAsPeer(ctx context.Context, node config.SupernodeInfo) (err error) {
//	if node.PID == "" {
//		logrus.Errorf("failed to register a supernode as peer,node PID is nil")
//		return errors.Wrap(err, "failed to register other supernode as peer,a nil supernode's PID")
//	}
//	if node.PID == etcd.config.GetSuperPID() {
//		return nil
//	}
//	// try whether this peer is already exist
//	if peer, err := etcd.PeerMgr.Get(ctx, node.PID); peer != nil || err != nil {
//		return nil
//	}
//	peerCreatRequest := &apiTypes.PeerCreateRequest{
//		IP:       strfmt.IPv4(node.IP),
//		HostName: strfmt.Hostname(node.HostName),
//		Port:     int32(node.DownloadPort),
//		PeerID:   node.PID,
//	}
//	_, err = etcd.PeerMgr.Register(ctx, peerCreatRequest)
//	if err != nil {
//		logrus.Errorf("failed to register other supernode %s as a peer,err %v", node.PID, err)
//		return err
//	}
//	logrus.Infof("success to register supernode as peer,peerID is %s", node.PID)
//	return nil
//}
//
//// deRegisterOtherSupernodePeer find and deregister all offline supernode
//// TODO(yunfeiyangbuaa)modify the code,make it more efficiency
//func (etcd *EtcdMgr) deRegisterOtherSupernodePeer(ctx context.Context) {
//	for _, pre := range etcd.preSupernodes {
//		mark := false
//		for _, now := range etcd.config.GetOtherSupernodeInfo() {
//			if pre.PID == now.PID {
//				mark = true
//				break
//			}
//		}
//		if mark == false {
//			etcd.deRegisterSupernodePeer(ctx, pre)
//			logrus.Info("supernodes %s are off,should delete the peer", pre.PID)
//		}
//	}
//}
//
//// deRegisterSupernodePeer deregister a supernode peer when this supernode is off
//func (etcd *EtcdMgr) deRegisterSupernodePeer(ctx context.Context, node config.SupernodeInfo) {
//	if err := etcd.ProgressMgr.DeletePeerStateByPeerID(ctx, node.PID); err != nil {
//		logrus.Errorf("failed to delete supernode peer peerState  %s,err: %v", node.PID, err)
//	}
//	if err := etcd.PeerMgr.DeRegister(ctx, node.PID); err != nil {
//		logrus.Errorf("failed to delete supernode peer peerMgr %s,err: %v", node.PID, err)
//	}
//}
