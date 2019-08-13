package ha

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"hash"
	"net"
	"net/http"
	"net/rpc"
)

type RpcManager struct {
	cfg          *config.Config
	CdnMgr       mgr.CDNMgr
	DfgetTaskMgr mgr.DfgetTaskMgr
	ProgressMgr  mgr.ProgressMgr
	TaskMgr      mgr.TaskMgr
}

type InitCdnRequest struct {
	TaskID        string
	TaskPieceSize int32
	NodePID       string
	NodeIP        string
}
type ReportPieceRequest struct {
	TaskID      string
	PieceNum    int
	Md5         string
	PieceStatus int
	CID         string
	SrcPID      string
	DstPID      string
}
type RpcUpdateTaskInfoRequest struct {
	CdnStatus  string
	FileLength int64
	RealMd5    string
	TaskID     string
}
type RpcReportCacheRequest struct {
	CdnStatus  string
	FileLength int64
	RealMd5    string
	StartNum   int
	Hash       hash.Hash
}

//clientID, taskID, dfgetTaskStatus
type RpcUpdateDfgetRequest struct {
	ClientID        string
	TaskID          string
	DfgetTaskStatus string
}

//ctx, taskID, clientID, dstPID, pieceNums[i], config.PieceRUNNING
type RpcUpdateClientProgressRequest struct {
	TaskID      string
	ClientID    string
	DstPID      string
	PieceNum    int
	PieceStatus int
}

func NewRpcMgr(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr) *RpcManager {
	rpcMgr := &RpcManager{
		CdnMgr:       CdnMgr,
		ProgressMgr:  ProgressMgr,
		DfgetTaskMgr: DfgetTaskMgr,
		TaskMgr:      TaskMgr,
		cfg:          cfg,
	}
	return rpcMgr
}

func StartRPCServer(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr) error {
	rpc.Register(NewRpcMgr(cfg, CdnMgr, DfgetTaskMgr, ProgressMgr, TaskMgr))
	rpc.HandleHTTP()
	rpcAddress := fmt.Sprintf("%s:%d", cfg.AdvertiseIP, cfg.HARpcPort)
	lis, err := net.Listen("tcp", rpcAddress)
	if err != nil {
		logrus.Errorf("failed to start a rpc server,err %v", err)
		return err
	}
	go http.Serve(lis, nil)
	fmt.Printf("open rpc server,address: %s\n", rpcAddress)
	return nil
}
func (rpc *RpcManager) RpcIntCdn(req InitCdnRequest, resp *bool) error {
	ctx := context.TODO()
	if req.NodePID == rpc.cfg.GetSuperPID() {
		return nil
	}
	cid := fmt.Sprintf("%s:%s~%s", "cdnnode", req.NodeIP, req.TaskID)
	path, err := rpc.CdnMgr.GetHTTPPath(ctx, req.TaskID)
	if err != nil {
		return err
	}
	if err := rpc.DfgetTaskMgr.Add(context.Background(), &types.DfGetTask{
		CID:       cid,
		Path:      path,
		PeerID:    req.NodePID,
		PieceSize: req.TaskPieceSize,
		Status:    types.DfGetTaskStatusWAITING,
		TaskID:    req.TaskID,
	}); err != nil {
		return errors.Wrapf(err, "failed to add cdn dfgetTask for taskID %s", req.TaskID)
	}
	rpc.ProgressMgr.InitProgress(context.Background(), req.TaskID, req.NodePID, cid)
	return nil
}

func (rpc *RpcManager) RpcReportPiece(req ReportPieceRequest, resp *bool) error {
	return rpc.CdnMgr.ReportPieceStatusForHA(context.TODO(), req.TaskID, req.CID, req.SrcPID, req.PieceNum, req.Md5, req.PieceStatus)
}

func (rpc *RpcManager) RpcUpdateTaskInfo(req RpcUpdateTaskInfoRequest, resp *bool) error {
	var (
		updateTask *types.TaskInfo
		err        error
	)
	if req.CdnStatus == types.TaskInfoCdnStatusFAILED {
		updateTask = &types.TaskInfo{
			CdnStatus: types.DfGetTaskStatusFAILED,
		}
	} else if req.CdnStatus == types.TaskInfoCdnStatusSUCCESS {
		updateTask = &types.TaskInfo{
			CdnStatus:  req.CdnStatus,
			FileLength: req.FileLength,
			RealMd5:    req.RealMd5,
		}
	} else {
		return errors.Wrapf(err, "failed to update taskinfo via rpc,unexpected req: %v", req)
	}
	if err = rpc.TaskMgr.Update(context.TODO(), req.TaskID, updateTask); err != nil {
		logrus.Errorf("failed to update task %v via rpc,err: %v", updateTask, req)
		return err
	}
	logrus.Infof("success to update task cdn via rpc %+v", updateTask)
	return nil
}

func (rpc *RpcManager) RpcUpdateDfgetTask(req RpcUpdateDfgetRequest, resp *bool) error {
	return rpc.DfgetTaskMgr.UpdateStatus(context.TODO(), req.ClientID, req.TaskID, req.DfgetTaskStatus)
}

func (rpc *RpcManager) RpcUpdateClientProgress(req RpcUpdateClientProgressRequest, resp *bool) error {
	return rpc.ProgressMgr.UpdateClientProgress(context.TODO(), req.TaskID, req.ClientID, req.DstPID, req.PieceNum, req.PieceStatus)
}

func (rpc *RpcManager) RpcPeerLoadAddOne(peerID string, resp *bool) error {
	peerState, err := rpc.ProgressMgr.GetPeerStateByPeerID(context.TODO(), peerID)
	if err != nil {
		return err
	}
	if peerState.ProducerLoad != nil {
		if peerState.ProducerLoad.Add(1) <= config.PeerUpLimit {
			return nil
		}
		peerState.ProducerLoad.Add(-1)
	}
	return nil
}

func (rpc *RpcManager) RpcUpdateProgress(req ReportPieceRequest, res *bool) error {
	return rpc.ProgressMgr.UpdateProgress(context.TODO(), req.TaskID, req.CID, req.SrcPID, req.DstPID, req.PieceNum, req.PieceStatus)
}

func (rpc *RpcManager) RpcReportCache(taskID string, resp *RpcReportCacheRequest) error {
	task, err := rpc.TaskMgr.Get(context.TODO(), taskID)
	if err != nil {
		return err
	}
	startNum, hash, _, err := rpc.CdnMgr.DetectCacheForHA(context.TODO(), task)
	if err != nil {
		return err
	}

	resp = &RpcReportCacheRequest{
		//CdnStatus:  updateTask.CdnStatus,
		//FileLength: updateTask.FileLength,
		//RealMd5:    updateTask.RealMd5,
		StartNum: startNum,
		Hash:     hash,
	}
	return nil
}
