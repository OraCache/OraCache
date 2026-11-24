package cplink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Client struct {
	connections []*grpc.ClientConn
	cluster     string
}

var _ controlplanev1.ControlPlaneAPIClient = (*Client)(nil)

// Dial creates a channel for each configured CP endpoint. The list is ordered
// so callers can prefer a local API while falling back to a remote one.
func Dial(addresses string) (*Client, error) {
	tlsConfig, err := TLSFromEnv("ORACACHE_CP", false)
	if err != nil {
		return nil, err
	}
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	client := &Client{cluster: Cluster()}
	if tlsConfig != nil {
		if client.cluster == "" {
			return nil, errors.New("CP mTLS requires ORACACHE_CP_CLUSTER_ID")
		}
		creds = credentials.NewTLS(tlsConfig)
	}
	for address := range strings.SplitSeq(addresses, ",") {
		address = strings.TrimSpace(address)
		if address == "" {
			_ = client.Close()
			return nil, errors.New("empty CP endpoint")
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("create CP channel: %w", err)
		}
		conn.Connect()
		client.connections = append(client.connections, conn)
	}
	return client, nil
}

func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.connections {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (c *Client) GetConfig(ctx context.Context, req *controlplanev1.GetConfigRequest, opts ...grpc.CallOption) (*controlplanev1.GetConfigResponse, error) {
	return invoke(ctx, c, opts, func(ctx context.Context, api controlplanev1.ControlPlaneAPIClient, options []grpc.CallOption) (*controlplanev1.GetConfigResponse, error) {
		return api.GetConfig(ctx, req, options...)
	})
}

func (c *Client) ReportHeartbeat(ctx context.Context, req *controlplanev1.HeartbeatRequest, opts ...grpc.CallOption) (*controlplanev1.HeartbeatResponse, error) {
	return invoke(ctx, c, opts, func(ctx context.Context, api controlplanev1.ControlPlaneAPIClient, options []grpc.CallOption) (*controlplanev1.HeartbeatResponse, error) {
		return api.ReportHeartbeat(ctx, req, options...)
	})
}

func invoke[T any](ctx context.Context, c *Client, opts []grpc.CallOption, call func(context.Context, controlplanev1.ControlPlaneAPIClient, []grpc.CallOption) (*T, error)) (*T, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	var last error
	for i, conn := range c.connections {
		attempt, cancel := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(c.connections)-i))
		var header metadata.MD
		options := append(append([]grpc.CallOption(nil), opts...), grpc.Header(&header))
		if c.cluster != "" {
			attempt = metadata.AppendToOutgoingContext(attempt, ClusterHeader, c.cluster)
		}
		reply, err := call(attempt, controlplanev1.NewControlPlaneAPIClient(conn), options)
		cancel()
		if err == nil {
			values := header.Get(ClusterHeader)
			if c.cluster != "" && (len(values) != 1 || values[0] != c.cluster) {
				return nil, status.Error(codes.FailedPrecondition, "unexpected Control Plane cluster")
			}
			return reply, nil
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			last = err
		default:
			return nil, err
		}
	}
	return nil, last
}
