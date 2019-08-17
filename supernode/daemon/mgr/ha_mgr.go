package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly/supernode/config"
)

// HaMgr is the interface to implement supernode Ha.
type HaMgr interface {

	// CloseHaManager closes the tool used to implement supernode ha.
	CloseHaManager(ctx context.Context) error

	// HADaemon is the etcd daemon progress to manager superodes cluster
	HADaemon(ctx context.Context) error

	SendPostCopy(ctx context.Context, req interface{}, path string, node config.SupernodeInfo) error
	//// SendGetCopy sends dfget's get request copy to other supernode.
	//SendGetCopy(ctx context.Context, path string, params string, node config.SupernodeInfo) error
	//
	//// SendPostCopy sends dfget's post request copy to other supernode.
	//SendPostCopy(ctx context.Context, req interface{}, path string, node config.SupernodeInfo) error
	//// SendRegisterRequestCopy senda register request copy to other supernodes.
	//SendRegisterRequestCopy(ctx context.Context, req *types.TaskRegisterRequest, triggerDownload bool)
}
