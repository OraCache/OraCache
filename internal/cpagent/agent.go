// Package cpagent implements the in-process Control Plane Agent embedded in an
// oracache-node process.
//
// The agent owns two loops: a heartbeat of local Raft partition statuses every
// 5 seconds and a config pull every 10 seconds (plus an immediate pull when a
// heartbeat response advertises a newer revision). It stores the last-known-good
// configuration atomically, never blocks Data Plane requests and never writes to
// a state machine directly.
package cpagent

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
)

const (
	DefaultHeartbeatInterval  = 5 * time.Second
	DefaultConfigPullInterval = 10 * time.Second
	DefaultRPCTimeout         = 3 * time.Second
	DefaultJitterFraction     = 0.10

	maxHeartbeatInterval = 5 * time.Minute
	maxBackoff           = 30 * time.Second
	minInterval          = time.Second
)

// StatusProvider returns a full snapshot of the node's local partition groups.
// Implementations read local Raft state and must not block on the network.
type StatusProvider interface {
	PartitionStatuses(ctx context.Context) ([]*controlplanev1.PartitionStatus, error)
}

// StatusProviderFunc adapts a function to StatusProvider.
type StatusProviderFunc func(ctx context.Context) ([]*controlplanev1.PartitionStatus, error)

func (f StatusProviderFunc) PartitionStatuses(ctx context.Context) ([]*controlplanev1.PartitionStatus, error) {
	return f(ctx)
}

// Clock supplies timers and wall time. Tests use a fake clock; production uses
// time.Timer.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors the subset of *time.Timer used by the agent.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer {
	return realTimer{timer: time.NewTimer(d)}
}

type realTimer struct{ timer *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.timer.C }
func (t realTimer) Stop() bool          { return t.timer.Stop() }

// Options configures an Agent. NodeID, Region, Client and Statuses are required.
type Options struct {
	NodeID   string
	Region   string
	Instance string

	Client   controlplanev1.ControlPlaneAPIClient
	Statuses StatusProvider
	Holder   *cpconfig.Holder

	Clock  Clock
	Logger *slog.Logger

	HeartbeatInterval  time.Duration
	ConfigPullInterval time.Duration
	RPCTimeout         time.Duration
	// JitterFraction spreads agents in [0,1]. Zero selects the 10% default.
	JitterFraction float64
	// DisableJitter turns jitter off for deterministic tests.
	DisableJitter bool
}

// Agent is the in-process CP Agent. It must not be copied after first use.
type Agent struct {
	nodeID   string
	region   string
	instance string

	client   controlplanev1.ControlPlaneAPIClient
	statuses StatusProvider
	holder   *cpconfig.Holder
	clock    Clock
	log      *slog.Logger

	heartbeatInterval  time.Duration
	configPullInterval time.Duration
	rpcTimeout         time.Duration
	jitterFraction     float64

	pullNow chan struct{}
}

// New validates options and returns an agent. It does not perform I/O.
func New(opts Options) (*Agent, error) {
	if opts.NodeID == "" {
		return nil, errors.New("oracache/cpagent: node id is required")
	}
	if opts.Region == "" {
		return nil, errors.New("oracache/cpagent: region is required")
	}
	if opts.Client == nil {
		return nil, errors.New("oracache/cpagent: client is required")
	}
	if opts.Statuses == nil {
		return nil, errors.New("oracache/cpagent: status provider is required")
	}
	if opts.JitterFraction < 0 || opts.JitterFraction > 1 {
		return nil, errors.New("oracache/cpagent: jitter fraction must be in [0,1]")
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Holder == nil {
		opts.Holder = cpconfig.NewHolder()
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if opts.ConfigPullInterval <= 0 {
		opts.ConfigPullInterval = DefaultConfigPullInterval
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = DefaultRPCTimeout
	}
	jitterFraction := opts.JitterFraction
	if jitterFraction == 0 && !opts.DisableJitter {
		jitterFraction = DefaultJitterFraction
	}
	if opts.DisableJitter {
		jitterFraction = 0
	}
	return &Agent{
		nodeID:             opts.NodeID,
		region:             opts.Region,
		instance:           opts.Instance,
		client:             opts.Client,
		statuses:           opts.Statuses,
		holder:             opts.Holder,
		clock:              opts.Clock,
		log:                opts.Logger,
		heartbeatInterval:  opts.HeartbeatInterval,
		configPullInterval: opts.ConfigPullInterval,
		rpcTimeout:         opts.RPCTimeout,
		jitterFraction:     jitterFraction,
		pullNow:            make(chan struct{}, 1),
	}, nil
}

// Config returns the last-known-good snapshot, or nil if none was applied.
func (a *Agent) Config() *cpconfig.Config { return a.holder.Current() }

// Revision returns the current known config revision, or 0.
func (a *Agent) Revision() uint64 { return a.holder.Revision() }

// Run starts heartbeat and config-pull loops and blocks until ctx is cancelled.
// CP errors are logged and retried with backoff; they never stop the Data Plane.
// Callers usually run it in a goroutine of the node process.
func (a *Agent) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("oracache/cpagent: nil context")
	}
	done := make(chan struct{}, 2)
	go func() {
		a.heartbeatLoop(ctx)
		done <- struct{}{}
	}()
	go func() {
		a.pullLoop(ctx)
		done <- struct{}{}
	}()
	<-done
	<-done
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	failures := 0
	for {
		next, err := a.sendHeartbeat(ctx)
		if ctx.Err() != nil {
			return
		}
		base := a.heartbeatInterval
		if next > 0 {
			base = clampDuration(next, minInterval, maxHeartbeatInterval)
		}
		if err != nil {
			failures++
			a.log.WarnContext(ctx, "cp heartbeat failed",
				"node_id", a.nodeID,
				"region", a.region,
				"failures", failures,
				"error", err,
			)
		} else {
			failures = 0
		}
		delay := backoffDelay(base, failures)
		delay = jitterDelay(delay, a.jitterFraction, a.nodeID)
		if !a.wait(ctx, delay) {
			return
		}
	}
}

func (a *Agent) pullLoop(ctx context.Context) {
	failures := 0
	for {
		err := a.pullConfig(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures++
			a.log.WarnContext(ctx, "cp config pull failed",
				"node_id", a.nodeID,
				"region", a.region,
				"failures", failures,
				"error", err,
			)
		} else {
			failures = 0
		}
		delay := backoffDelay(a.configPullInterval, failures)
		delay = jitterDelay(delay, a.jitterFraction, a.nodeID)
		if !a.waitForPull(ctx, delay) {
			return
		}
	}
}

// sendHeartbeat reports a full snapshot and returns an optional server-advised
// interval. It returns an error if status collection or the RPC fails.
func (a *Agent) sendHeartbeat(ctx context.Context) (time.Duration, error) {
	statuses, err := a.statuses.PartitionStatuses(ctx)
	if err != nil {
		return 0, fmt.Errorf("collect local partition statuses: %w", err)
	}
	revision := a.holder.Revision()
	req := &controlplanev1.HeartbeatRequest{
		NodeId:         a.nodeID,
		Region:         a.region,
		Instance:       a.instance,
		SentAt:         timestamppb.New(a.clock.Now().UTC()),
		ConfigRevision: revision,
		Partitions:     statuses,
	}
	rpcCtx, cancel := context.WithTimeout(ctx, a.rpcTimeout)
	defer cancel()
	resp, err := a.client.ReportHeartbeat(rpcCtx, req)
	if err != nil {
		return 0, fmt.Errorf("report heartbeat: %w", err)
	}
	if resp == nil {
		return 0, errors.New("report heartbeat: empty response")
	}
	if !resp.GetAccepted() {
		return 0, errors.New("report heartbeat: rejected by CP")
	}
	if resp.GetConfigRevision() > revision {
		a.triggerPull()
	}
	if ms := resp.GetNextHeartbeatAfterMs(); ms > 0 {
		return time.Duration(ms) * time.Millisecond, nil
	}
	return 0, nil
}

// pullConfig fetches and atomically applies a newer config revision. A failed
// or invalid pull keeps the previous last-known-good snapshot.
func (a *Agent) pullConfig(ctx context.Context) error {
	known := a.holder.Revision()
	req := &controlplanev1.GetConfigRequest{
		NodeId:        a.nodeID,
		KnownRevision: known,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE,
	}
	rpcCtx, cancel := context.WithTimeout(ctx, a.rpcTimeout)
	defer cancel()
	resp, err := a.client.GetConfig(rpcCtx, req)
	if err != nil {
		return fmt.Errorf("get config: %w", err)
	}
	if resp == nil {
		return errors.New("get config: empty response")
	}
	if resp.GetRevision() < known {
		return fmt.Errorf("get config: revision went backwards from %d to %d", known, resp.GetRevision())
	}
	if resp.GetRevision() == known {
		return nil
	}
	if resp.GetConfig() == nil {
		return errors.New("get config: new revision without config body")
	}
	if resp.GetConfig().GetRevision() != resp.GetRevision() {
		return fmt.Errorf("get config: config revision %d does not match response revision %d",
			resp.GetConfig().GetRevision(), resp.GetRevision())
	}
	next, err := cpconfig.FromProto(resp.GetConfig())
	if err != nil {
		return fmt.Errorf("get config: invalid config: %w", err)
	}
	if err := a.holder.Apply(next); err != nil {
		return fmt.Errorf("get config: apply config: %w", err)
	}
	a.log.Info("cp config applied",
		"node_id", a.nodeID,
		"region", a.region,
		"config_revision", next.Revision,
	)
	return nil
}

func (a *Agent) triggerPull() {
	select {
	case a.pullNow <- struct{}{}:
	default:
	}
}

func (a *Agent) wait(ctx context.Context, delay time.Duration) bool {
	timer := a.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C():
		return true
	}
}

func (a *Agent) waitForPull(ctx context.Context, delay time.Duration) bool {
	timer := a.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-a.pullNow:
		return true
	case <-timer.C():
		return true
	}
}

func backoffDelay(base time.Duration, failures int) time.Duration {
	if failures <= 0 {
		return base
	}
	shift := failures - 1
	if shift > 5 {
		shift = 5
	}
	delay := base << shift
	if base < maxBackoff && delay > maxBackoff {
		delay = maxBackoff
	}
	return delay
}

// jitterDelay spreads agents so they do not all hit CP at the same instant.
// The offset is deterministic per node, making tests and operational debugging
// reproducible; it is not used for data routing.
func jitterDelay(base time.Duration, fraction float64, seed string) time.Duration {
	if fraction <= 0 || base <= 0 {
		return base
	}
	span := int64(float64(base) * fraction)
	if span <= 0 {
		return base
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	offset := int64(uint64(h.Sum32())%uint64(2*span+1)) - span
	return base + time.Duration(offset)
}

func clampDuration(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}
