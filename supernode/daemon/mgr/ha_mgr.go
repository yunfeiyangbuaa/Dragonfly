package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly/apis/types"
)

// HaMgr is the interface to implement supernode Ha.
type HaMgr interface {

	// CloseHaManager closes the tool used to implement supernode ha.
	CloseHaManager() error

	//// SendGetCopy sends dfget's get request copy to standby supernode.
	//SendGetCopy(params string) error
	//
	//// SendPostCopy sends dfget's post request copy to standby supernode.
	//SendPostCopy(req interface{}, path string) error

	//AddOtherSupernodesCdnResource(task *types.TaskInfo)

	HADaemon(ctx context.Context) error

	SendRegisterRequestCopy(req *types.TaskRegisterRequest, triggerDownload bool)
}
