package dfgettask

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	errorType "github.com/dragonflyoss/Dragonfly/common/errors"
	cutil "github.com/dragonflyoss/Dragonfly/common/util"
	"github.com/dragonflyoss/Dragonfly/supernode/daemon/mgr"
	dutil "github.com/dragonflyoss/Dragonfly/supernode/daemon/util"
	"github.com/pkg/errors"
)

var _ mgr.DfgetTaskMgr = &Manager{}

// Manager is an implementation of the interface of DfgetTaskMgr.
type Manager struct {
	dfgetTaskStore *dutil.Store
	ptoc           *cutil.SyncMap
}

// NewManager returns a new Manager.
func NewManager() (*Manager, error) {
	return &Manager{
		dfgetTaskStore: dutil.NewStore(),
		ptoc:           cutil.NewSyncMap(),
	}, nil
}

// Add a new dfgetTask, we use clientID and taskID to identify a dfgetTask uniquely.
// ClientID should be generated by dfget, supernode will use it directly.
// NOTE: We should create a new dfgetTask for each download process,
//       even if the downloads initiated by the same machine.
func (dtm *Manager) Add(ctx context.Context, dfgetTask *types.DfGetTask) error {
	if cutil.IsEmptyStr(dfgetTask.Path) {
		return errors.Wrapf(errorType.ErrEmptyValue, "Path")
	}

	if cutil.IsEmptyStr(dfgetTask.PeerID) {
		return errors.Wrapf(errorType.ErrEmptyValue, "PeerID")
	}

	key, err := generateKey(dfgetTask.CID, dfgetTask.TaskID)
	if err != nil {
		return err
	}

	// the default status of DfgetTask is WAITING
	if cutil.IsEmptyStr(dfgetTask.Status) {
		dfgetTask.Status = types.DfGetTaskStatusWAITING
	}

	// TODO: should we verify that the peerID is valid here.

	dtm.ptoc.Add(generatePeerKey(dfgetTask.PeerID, dfgetTask.TaskID), dfgetTask.CID)
	dtm.dfgetTaskStore.Put(key, dfgetTask)
	return nil
}

// Get a dfgetTask info with specified clientID and taskID.
func (dtm *Manager) Get(ctx context.Context, clientID, taskID string) (dfgetTask *types.DfGetTask, err error) {
	return dtm.getDfgetTask(clientID, taskID)
}

// GetCIDByPeerIDAndTaskID returns cid with specified peerID and taskID.
func (dtm *Manager) GetCIDByPeerIDAndTaskID(ctx context.Context, peerID, taskID string) (string, error) {
	return dtm.ptoc.GetAsString(generatePeerKey(peerID, taskID))
}

// List returns the list of dfgetTask.
func (dtm *Manager) List(ctx context.Context, filter map[string]string) (dfgetTaskList []*types.DfGetTask, err error) {
	return nil, nil
}

// Delete a dfgetTask with clientID and taskID.
func (dtm *Manager) Delete(ctx context.Context, clientID, taskID string) error {
	key, err := generateKey(clientID, taskID)
	if err != nil {
		return err
	}

	dfgetTask, err := dtm.getDfgetTask(clientID, taskID)
	if err != nil {
		return err
	}
	dtm.ptoc.Delete(generatePeerKey(dfgetTask.PeerID, dfgetTask.TaskID))

	return dtm.dfgetTaskStore.Delete(key)
}

// UpdateStatus update the status of dfgetTask with specified clientID and taskID.
func (dtm *Manager) UpdateStatus(ctx context.Context, clientID, taskID, status string) error {
	dfgetTask, err := dtm.getDfgetTask(clientID, taskID)
	if err != nil {
		return err
	}

	if dfgetTask.Status != types.DfGetTaskStatusSUCCESS {
		dfgetTask.Status = status
	}

	return nil
}

// getDfgetTask gets a DfGetTask from dfgetTaskStore with specified clientID and taskID.
func (dtm *Manager) getDfgetTask(clientID, taskID string) (*types.DfGetTask, error) {
	key, err := generateKey(clientID, taskID)
	if err != nil {
		return nil, err
	}

	v, err := dtm.dfgetTaskStore.Get(key)
	if err != nil {
		return nil, err
	}

	if dfgetTask, ok := v.(*types.DfGetTask); ok {
		return dfgetTask, nil
	}
	return nil, errors.Wrapf(errorType.ErrConvertFailed, "clientID: %s, taskID: %s: %v", clientID, taskID, v)
}

// generateKey generates a key for a dfgetTask.
func generateKey(cID, taskID string) (string, error) {
	if cutil.IsEmptyStr(cID) {
		return "", errors.Wrapf(errorType.ErrEmptyValue, "cID")
	}

	if cutil.IsEmptyStr(taskID) {
		return "", errors.Wrapf(errorType.ErrEmptyValue, "taskID")
	}

	return fmt.Sprintf("%s%s%s", cID, "@", taskID), nil
}

func generatePeerKey(peerID, taskID string) string {
	return fmt.Sprintf("%s@%s", peerID, taskID)
}
