package ha

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	PeerMgr      mgr.PeerMgr
}

type ReportPieceRequest struct {
	TaskID      string
	PieceNum    int
	Md5         string
	PieceStatus int
	CID         string
	SrcPID      string
	DstCID      string
}

type RpcUpdateTaskInfoRequest struct {
	CdnStatus  string
	FileLength int64
	RealMd5    string
	TaskID     string
	CDNPeerID  string
}

type RpcGetPieceRequest struct {
	DfgetTaskStatus string
	PieceRange      string
	PieceResult     string
	TaskId          string
	Cid             string
	DstCID        string
}

type RpcGetPieceResponse struct {
	IsFinished bool
	Data       []*types.PieceInfo
	ErrCode    int
	ErrMsg     string
}

type RpcServerDownRequest struct{
	TaskID string
	CID  string
}
func NewRpcMgr(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr,PeerMgr mgr.PeerMgr) *RpcManager {
	rpcMgr := &RpcManager{
		CdnMgr:       CdnMgr,
		ProgressMgr:  ProgressMgr,
		DfgetTaskMgr: DfgetTaskMgr,
		TaskMgr:      TaskMgr,
		PeerMgr:      PeerMgr,
		cfg:          cfg,
	}
	return rpcMgr
}

func StartRPCServer(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr,PeerMgr mgr.PeerMgr) error {
	rpc.Register(NewRpcMgr(cfg, CdnMgr, DfgetTaskMgr, ProgressMgr, TaskMgr,PeerMgr))
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


func (rpc *RpcManager) RpcUpdateProgress(req ReportPieceRequest, res *bool) error {
	dstDfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), req.DstCID,req.TaskID)
	if err != nil {
		return err
	}
	return rpc.ProgressMgr.UpdateProgress(context.TODO(), req.TaskID, req.CID, req.SrcPID, dstDfgetTask.PeerID, req.PieceNum, req.PieceStatus)
}

func (rpc *RpcManager) RpcGetTaskInfo(req string, resp *RpcUpdateTaskInfoRequest) error {
	taskInfo, err := rpc.TaskMgr.Get(context.TODO(), req)
	if err != nil {
		return err
	}
	resp.CdnStatus = taskInfo.CdnStatus
	resp.FileLength = taskInfo.FileLength
	resp.RealMd5 = taskInfo.RealMd5
	resp.CDNPeerID = taskInfo.CDNPeerID
	resp.TaskID = taskInfo.ID

	return nil
}

func (rpc *RpcManager) RpcGetPiece(req RpcGetPieceRequest, resp *RpcGetPieceResponse) error {
	piecePullRequest := &types.PiecePullRequest{
		DfgetTaskStatus: req.DfgetTaskStatus,
		PieceRange:      req.PieceRange,
		PieceResult:     req.PieceResult,
		DstCid:          req.DstCID,
	}
	if !stringutils.IsEmptyStr(req.DstCID) {
		dstDfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), req.Cid,req.TaskId)
		if err != nil {
			logrus.Warnf("failed to get dfget task by dstCID(%s) and taskID(%s), and the srcCID is %s, err: %v",
				req.DstCID, req.TaskId, req.Cid, err)
		} else {
			piecePullRequest.DstPID = dstDfgetTask.PeerID
		}
	}
	isFinished, data, err := rpc.TaskMgr.GetPieces(context.TODO(), req.TaskId, req.Cid, piecePullRequest)
	if err != nil {
		e, ok := errors.Cause(err).(errortypes.DfError)
		if ok{
			resp.ErrCode=e.Code
			resp.ErrMsg=e.Msg
		}
	}else{
		pieceInfos, _ := data.([]*types.PieceInfo)
		resp.Data = pieceInfos
		resp.IsFinished = isFinished
	}
	return nil
}

func (rpc *RpcManager)RpcDfgetServerDown(request RpcServerDownRequest,resp *bool)error{
	dfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), request.CID, request.TaskID)
	if err != nil {
		return err
	}

	if err :=  rpc.ProgressMgr.DeletePieceProgressByCID(context.TODO(), request.TaskID,request.CID); err != nil {
		return err
	}

	if err :=  rpc.ProgressMgr.DeletePeerStateByPeerID(context.TODO(), dfgetTask.PeerID); err != nil {
		return err
	}

	if err :=  rpc.PeerMgr.DeRegister(context.TODO(), dfgetTask.PeerID); err != nil {
		return err
	}

	if err :=  rpc.DfgetTaskMgr.Delete(context.TODO(),request.CID, request.TaskID); err != nil {
		return err
	}
	return nil
}

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
