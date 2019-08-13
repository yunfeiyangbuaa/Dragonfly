package mgr

import (
	"context"
	"hash"

	"github.com/dragonflyoss/Dragonfly/apis/types"
)

// CDNMgr as an interface defines all operations against CDN and
// operates on the underlying files stored on the local disk, etc.
type CDNMgr interface {
	// TriggerCDN will trigger CDN to download the file from sourceUrl.
	// It includes the following steps:
	// 1). download the source file
	// 2). write the file to disk
	//
	// In fact, it's a very time consuming operation.
	// So if not necessary, it should usually be executed concurrently.
	// In addition, it's not thread-safe.
	TriggerCDN(ctx context.Context, taskInfo *types.TaskInfo) (*types.TaskInfo, error)

	// GetHTTPPath returns the http download path of taskID.
	GetHTTPPath(ctx context.Context, taskID string) (path string, err error)

	// GetStatus get the status of the file.
	GetStatus(ctx context.Context, taskID string) (cdnStatus string, err error)

	// Delete the file from disk with specified taskID.
	Delete(ctx context.Context, taskID string) error

	ReportPieceStatusForHA(ctx context.Context, taskID string, cID string, pID string, pieceNum int, md5 string, pieceStatus int) (err error)

	DetectCacheForHA(ctx context.Context, task *types.TaskInfo) (int, hash.Hash, *types.TaskInfo, error)
}
