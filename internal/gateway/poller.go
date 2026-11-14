package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"google.golang.org/grpc"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
)

// DefaultPollInterval is the maximum age of a Gateway routing map before the
// next CP pull. NOT_LEADER hints trigger an immediate Refresh.
const DefaultPollInterval = 10 * time.Second

// DefaultRPCTimeout bounds one CP API call; CP is never in the hot path.
const DefaultRPCTimeout = 5 * time.Second

// ConfigClient is the subset of ControlPlaneAPI used by the Gateway.
type ConfigClient interface {
	GetConfig(ctx context.Context, in *controlplanev1.GetConfigRequest, opts ...grpc.CallOption) (*controlplanev1.GetConfigResponse, error)
}

// Poller keeps a Router on the last-known-good CP configuration.
//
// A failed refresh does not clear the routing map and does not stop the
// Gateway: Data Plane requests continue on the previous snapshot.
type Poller struct {
	Client     ConfigClient
	Router     *Router
	NodeID     string
	Interval   time.Duration
	RPCTimeout time.Duration
	Logger     *slog.Logger
}

// NewPoller returns a Gateway config poller with production defaults.
func NewPoller(client ConfigClient, router *Router, nodeID string) *Poller {
	return &Poller{
		Client:     client,
		Router:     router,
		NodeID:     nodeID,
		Interval:   DefaultPollInterval,
		RPCTimeout: DefaultRPCTimeout,
		Logger:     slog.Default(),
	}
}

// Refresh performs exactly one config pull. It is also the immediate-refresh
// path after a NOT_LEADER/NO_QUORUM hint.
func (p *Poller) Refresh(ctx context.Context) error {
	if p.Client == nil {
		return errors.New("oracache/gateway: nil config client")
	}
	if p.Router == nil {
		return errors.New("oracache/gateway: nil router")
	}
	if p.NodeID == "" {
		return errors.New("oracache/gateway: empty gateway node id")
	}
	timeout := p.RPCTimeout
	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	known := p.Router.Revision()
	resp, err := p.Client.GetConfig(rpcCtx, &controlplanev1.GetConfigRequest{
		NodeId:        p.NodeID,
		KnownRevision: known,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	})
	if err != nil {
		return fmt.Errorf("oracache/gateway: get config: %w", err)
	}
	if resp == nil {
		return errors.New("oracache/gateway: empty config response")
	}
	if resp.GetRevision() < known {
		return fmt.Errorf("oracache/gateway: CP revision went backwards: known %d, got %d", known, resp.GetRevision())
	}
	if resp.GetRevision() == known {
		return nil
	}
	if resp.GetConfig() == nil {
		return errors.New("oracache/gateway: CP returned a new revision without a config")
	}
	if resp.GetConfig().GetRevision() != resp.GetRevision() {
		return fmt.Errorf("oracache/gateway: config revision %d does not match response revision %d",
			resp.GetConfig().GetRevision(), resp.GetRevision())
	}
	cfg, err := cpconfig.FromProto(resp.GetConfig())
	if err != nil {
		return fmt.Errorf("oracache/gateway: invalid config: %w", err)
	}
	if err := p.Router.Apply(cfg); err != nil {
		return fmt.Errorf("oracache/gateway: apply config: %w", err)
	}
	p.logger().Info("gateway routing map updated",
		"node_id", p.NodeID,
		"region", p.Router.Region(),
		"config_revision", cfg.Revision,
	)
	return nil
}

// Run pulls the config immediately, then periodically until ctx is cancelled.
// Refresh errors are logged and retried; the current routing map is preserved.
func (p *Poller) Run(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	if err := p.Refresh(ctx); err != nil && ctx.Err() == nil {
		p.logger().WarnContext(ctx, "gateway config refresh failed", "node_id", p.NodeID, "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.Refresh(ctx); err != nil && ctx.Err() == nil {
				p.logger().WarnContext(ctx, "gateway config refresh failed", "node_id", p.NodeID, "error", err)
			}
		}
	}
}

func (p *Poller) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
