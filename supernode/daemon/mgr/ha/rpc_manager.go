package ha

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	"github.com/go-openapi/strfmt"
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

type RpcReportPieceRequest struct {
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
	DstCID          string
}

type RpcGetPieceResponse struct {
	IsFinished bool
	Data       []*types.PieceInfo
	ErrCode    int
	ErrMsg     string
}

type RpcServerDownRequest struct {
	TaskID string
	CID    string
}
type RpcAddSupernodeWatchRequest struct{
	TaskID string
	SupernodePID  string
}
func NewRpcMgr(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr, PeerMgr mgr.PeerMgr) *RpcManager {
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

func StartRPCServer(cfg *config.Config, CdnMgr mgr.CDNMgr, DfgetTaskMgr mgr.DfgetTaskMgr, ProgressMgr mgr.ProgressMgr, TaskMgr mgr.TaskMgr, PeerMgr mgr.PeerMgr) error {
	rpc.Register(NewRpcMgr(cfg, CdnMgr, DfgetTaskMgr, ProgressMgr, TaskMgr, PeerMgr))
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

func (rpc *RpcManager) RpcUpdateProgress(req RpcReportPieceRequest, res *bool) error {
	dstDfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), req.DstCID, req.TaskID)
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
		dstDfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), req.Cid, req.TaskId)
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
		if ok {
			resp.ErrCode = e.Code
			resp.ErrMsg = e.Msg
		}
	} else {
		pieceInfos, _ := data.([]*types.PieceInfo)
		resp.Data = pieceInfos
		resp.IsFinished = isFinished
	}
	return nil
}

func (rpc *RpcManager) RpcDfgetServerDown(request RpcServerDownRequest, resp *bool) error {
	dfgetTask, err := rpc.DfgetTaskMgr.Get(context.TODO(), request.CID, request.TaskID)
	if err != nil {
		return err
	}
	if err := rpc.ProgressMgr.DeletePieceProgressByCID(context.TODO(), request.TaskID, request.CID); err != nil {
		return err
	}
	if err := rpc.ProgressMgr.DeletePeerStateByPeerID(context.TODO(), dfgetTask.PeerID); err != nil {
		return err
	}
	if err := rpc.PeerMgr.DeRegister(context.TODO(), dfgetTask.PeerID); err != nil {
		return err
	}
	if err := rpc.DfgetTaskMgr.Delete(context.TODO(), request.CID, request.TaskID); err != nil {
		return err
	}
	return nil
}

func (rpc *RpcManager) RpcOnlyTriggerCDNDownload(req types.TaskRegisterRequest, resp *bool) error {
	if err := req.Validate(strfmt.NewFormats()); err != nil {
		return errors.Wrap(errortypes.ErrInvalidValue, err.Error())
	}
	taskCreateRequest := &types.TaskCreateRequest{
		CID:         req.CID,
		CallSystem:  req.CallSystem,
		Dfdaemon:    req.Dfdaemon,
		Headers:     netutils.ConvertHeaders(req.Headers),
		Identifier:  req.Identifier,
		Md5:         req.Md5,
		Path:        req.Path,
		PeerID:      "",
		RawURL:      req.RawURL,
		TaskURL:     req.TaskURL,
		SupernodeIP: req.SuperNodeIP,
	}
	if err := rpc.TaskMgr.OnlyTriggerDownload(context.TODO(), taskCreateRequest, &req); err != nil {
		logrus.Errorf("failed to trigger CDN download by rpc,err: %v", err)
		return err
	}
	return nil
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
	fmt.Println("rpc update task",req)
	return nil
}
func (rpc *RpcManager)RpcAddSupernodeWatch(req RpcAddSupernodeWatchRequest,resp *bool)error{
	task, err := rpc.TaskMgr.Get(context.TODO(),req.TaskID)
	if err != nil {
		return err
	}
	task.NotifySupernodesPID=append(task.NotifySupernodesPID, req.SupernodePID)
	fmt.Println("success add a supernode watch",req,task.NotifySupernodesPID)
	return nil
}