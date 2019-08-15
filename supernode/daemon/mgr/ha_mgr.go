package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
)

// HaMgr is the interface to implement supernode Ha.
type HaMgr interface {

	// CloseHaManager closes the tool used to implement supernode ha.
	CloseHaManager() error

	// SendGetCopy sends dfget's get request copy to standby supernode.
	SendGetCopy(path string, params string,node config.SupernodeInfo)error

	// SendPostCopy sends dfget's post request copy to standby supernode.
	SendPostCopy(req interface{}, path string, node config.SupernodeInfo) error

	//AddOtherSupernodesCdnResource(task *types.TaskInfo)

	HADaemon(ctx context.Context) error

	SendRegisterRequestCopy(req *types.TaskRegisterRequest, triggerDownload bool)
}
