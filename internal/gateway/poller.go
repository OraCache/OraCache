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

// DefaultPollInterval — максимальный возраст routing map Gateway до следующего
// pull из CP. Hints NOT_LEADER вызывают немедленный Refresh.
const DefaultPollInterval = 10 * time.Second

// DefaultRPCTimeout ограничивает один вызов CP API; CP никогда не в горячем пути.
const DefaultRPCTimeout = 5 * time.Second

// ConfigClient — подмножество ControlPlaneAPI, используемое Gateway.
type ConfigClient interface {
	GetConfig(ctx context.Context, in *controlplanev1.GetConfigRequest, opts ...grpc.CallOption) (*controlplanev1.GetConfigResponse, error)
}

// Poller поддерживает Router на last-known-good конфигурации CP.
//
// Неудачный refresh не очищает routing map и не останавливает Gateway:
// запросы Data Plane продолжают работать по предыдущему snapshot.
type Poller struct {
	Client     ConfigClient
	Router     *Router
	NodeID     string
	Interval   time.Duration
	RPCTimeout time.Duration
	Logger     *slog.Logger
	Metrics    *Metrics
	refresh    chan struct{}
}

// NewPoller создаёт Gateway config poller с production-настройками по умолчанию.
func NewPoller(client ConfigClient, router *Router, nodeID string) *Poller {
	return &Poller{
		Client:     client,
		Router:     router,
		NodeID:     nodeID,
		Interval:   DefaultPollInterval,
		RPCTimeout: DefaultRPCTimeout,
		Logger:     slog.Default(),
		refresh:    make(chan struct{}, 1),
	}
}

// RequestRefresh coalesces leader-change signals without doing network IO.
func (p *Poller) RequestRefresh() {
	select {
	case p.refresh <- struct{}{}:
	default:
	}
}

// Refresh выполняет ровно один config pull. Это также путь немедленного
// обновления после hint NOT_LEADER/NO_QUORUM.
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
	result := "rejected"
	if p.Metrics != nil {
		defer func() {
			if result != "rpc_error" && result != "rejected" {
				p.Metrics.lastSuccess.WithLabelValues().SetToCurrentTime()
			}
			p.Metrics.refreshes.WithLabelValues(result).Inc()
		}()
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
		result = "rpc_error"
		return fmt.Errorf("oracache/gateway: get config: %w", err)
	}
	if resp == nil {
		return errors.New("oracache/gateway: empty config response")
	}
	if resp.GetRevision() < known {
		return fmt.Errorf("oracache/gateway: CP revision went backwards: known %d, got %d", known, resp.GetRevision())
	}
	if resp.GetConfig() == nil {
		if resp.GetRevision() == known {
			result = "unchanged"
			return nil
		}
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
	if err := p.Router.ApplyView(cfg); err != nil {
		return fmt.Errorf("oracache/gateway: apply live view: %w", err)
	}
	if resp.GetRevision() == known {
		result = "applied_same_revision"
		p.logger().Debug("gateway live view refreshed",
			"node_id", p.NodeID,
			"region", p.Router.Region(),
			"config_revision", cfg.Revision,
		)
	} else {
		result = "applied_new_revision"
		p.logger().Info("gateway routing map updated",
			"node_id", p.NodeID,
			"region", p.Router.Region(),
			"config_revision", cfg.Revision,
		)
	}
	return nil
}

// Run сразу забирает конфигурацию, затем периодически до отмены ctx.
// Ошибки Refresh логируются и повторяются; текущий routing map сохраняется.
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
		case <-p.refresh:
		}
		if err := p.Refresh(ctx); err != nil && ctx.Err() == nil {
			p.logger().WarnContext(ctx, "gateway config refresh failed", "node_id", p.NodeID, "error", err)
		}
	}
}

func (p *Poller) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
