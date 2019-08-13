package ha

import (
	"context"
	"github.com/go-openapi/strfmt"
)

// Tool is an interface that use etcd/zookeeper/yourImplement tools to make supernode be standby or active.
type Tool interface {
	// WatchStandbySupernodesChange watches the standby supernodes.supernode will be notified if the standby superodes changes
	WatchSupernodesChange(ctx context.Context, key string) error

	// Close closes the tool.
	Close(ctx context.Context) error

	// StopKeepHeartBeat stops sending heart beat to cancel a etcd lease
	StopKeepHeartBeat(ctx context.Context) error

	SendSupernodesInfo(ctx context.Context, key, ip, pID string, listenPort, downloadPort, rpcPort int, hostName strfmt.Hostname, timeout int64) error
}
