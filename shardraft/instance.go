package shardraft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type result struct {
	message    proto.Message
	membership *MembershipResult
	err        error
}
type request struct {
	ctx    context.Context
	op     operation
	data   []byte
	reply  chan result
	change *MembershipChange
}
type pendingRequest struct {
	request
	readIndex     uint64
	readReady     bool
	readStarted   time.Time
	adminVerified bool
}
type outgoing struct {
	data     []byte
	snapshot bool
}
type delivery struct {
	to       uint64
	snapshot bool
	err      error
}

type heartbeatSnapshot struct {
	status     Status
	membership Membership
}

// Instance owns one peer and one cache. All Raft and cache calls are serialized
// by Run. An Instance is single-use: restarting without term/vote is unsafe.
type Instance struct {
	cfg              Config
	cache            StateMachine
	transport        Transport
	raw              *raft.RawNode
	storage          *raft.MemoryStorage
	peers            map[uint64]Peer
	requests         chan request
	incoming         chan raftpb.Message
	deliveries       chan delivery
	outbound         map[uint64]chan outgoing
	stop             chan struct{}
	done             chan struct{}
	mu               sync.Mutex
	lifecycle        int // 0 constructed, 1 running, 2 stopped
	stopOnce         sync.Once
	observed         atomic.Pointer[Status]
	heartbeat        atomic.Pointer[heartbeatSnapshot]
	membershipEvents chan struct{}
	known            atomic.Value // immutable map used by concurrent Step calls
	// Fields below belong exclusively to Run.
	pending       map[requestID]*pendingRequest
	sequence      uint64
	applied       uint64
	snapshotIndex uint64
	lastIndex     uint64
	conf          raftpb.ConfState
	configIndex   uint64
	removed       map[uint64]bool
	operations    map[string]membershipRecord
	proposing     *MembershipChange
	proposalTerm  uint64
	workerCtx     context.Context
	workers       sync.WaitGroup
}

// NewInstance bootstraps a group, or prepares an empty joining member.
// Every initial peer must receive the same group identity and peer list.
// The cache must be empty. No goroutines or network listeners are started here.
func NewInstance(cfg Config, cache StateMachine, transport Transport) (*Instance, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	if cache == nil || transport == nil {
		return nil, fmt.Errorf("cache and transport are required")
	}
	storage := raft.NewMemoryStorage()
	raw, err := raft.NewRawNode(&raft.Config{
		ID: cfg.PeerID, Storage: storage,
		ElectionTick: cfg.ElectionTicks, HeartbeatTick: cfg.HeartbeatTicks,
		MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 16 << 20,
		MaxUncommittedEntriesSize: 64 << 20, MaxInflightMsgs: 64,
		CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe,
		DisableProposalForwarding: true, Logger: cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	n := &Instance{
		cfg: cfg, cache: cache, transport: transport, raw: raw, storage: storage,
		peers: make(map[uint64]Peer), requests: make(chan request),
		incoming: make(chan raftpb.Message, 64), deliveries: make(chan delivery, 64),
		outbound: make(map[uint64]chan outgoing), stop: make(chan struct{}), done: make(chan struct{}),
		pending: make(map[requestID]*pendingRequest),
		removed: make(map[uint64]bool), operations: make(map[string]membershipRecord),
		membershipEvents: make(chan struct{}, 1),
	}
	initial := make([]raft.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if resolver, ok := transport.(interface{ PeerAddress(uint64) string }); ok && p.RaftAddress == "" {
			p.RaftAddress = resolver.PeerAddress(p.ID)
		}
		if validator, ok := transport.(MembershipTransport); ok {
			if err := validator.ValidatePeer(p); err != nil {
				return nil, err
			}
		}
		n.peers[p.ID] = p
		data, _ := json.Marshal(p)
		if len(data) > 4096 {
			return nil, fmt.Errorf("resolved peer metadata exceeds 4096 bytes")
		}
		initial = append(initial, raft.Peer{ID: p.ID, Context: data})
		if p.ID != cfg.PeerID {
			n.outbound[p.ID] = make(chan outgoing, 8)
		}
	}
	if !cfg.Join {
		if err := raw.Bootstrap(initial); err != nil {
			return nil, err
		}
	}
	n.publishKnown()
	n.publish(false, "")
	return n, nil
}

// Status returns the latest diagnostic snapshot, including in-progress apply.
func (n *Instance) Status() Status { return *n.observed.Load() }

// HeartbeatStatus returns the snapshot from the end of a Raft owner iteration.
// It preserves the readiness observation used by Control Plane routing.
func (n *Instance) HeartbeatStatus() Status { return n.heartbeat.Load().status }

// HeartbeatState returns the status and membership from the same owner iteration.
func (n *Instance) HeartbeatState() (Status, Membership) {
	snapshot := n.heartbeat.Load()
	return snapshot.status, snapshot.membership.clone()
}

func (n *Instance) snapshot(running bool, lastError string) *Status {
	s := n.raw.BasicStatus()
	return &Status{
		RegionID: n.cfg.RegionID, ShardID: n.cfg.ShardID, GroupID: n.cfg.GroupID, PeerID: n.cfg.PeerID,
		Role: s.RaftState.String(), Term: s.Term, LeaderID: s.Lead,
		CommitIndex: s.Commit, AppliedIndex: n.applied, SnapshotIndex: n.snapshotIndex, LastIndex: n.lastIndex,
		IsLearner: n.isLearner(n.cfg.PeerID), Member: n.isVoter(n.cfg.PeerID),
		Ready:   running && n.canServe() && s.Lead != 0 && n.applied >= s.Commit,
		Running: running, LastError: lastError,
	}
}

func (n *Instance) publish(running bool, lastError string) {
	next := &heartbeatSnapshot{status: *n.snapshot(running, lastError)}
	previous := n.heartbeat.Load()
	if previous != nil {
		next.membership = previous.membership
	}
	// Reuse membership between configuration or leadership changes.
	m := next.membership
	changed := previous == nil || m.Index != n.configIndex || m.Term != next.status.Term || m.LeaderID != next.status.LeaderID
	if changed {
		next.membership = n.membershipView()
	}
	n.heartbeat.Store(next)
	n.observed.Store(&next.status)
	if changed && next.status.Role == raft.StateLeader.String() && n.configIndex != 0 &&
		(previous == nil || m.Index != n.configIndex || m.LeaderID != n.cfg.PeerID) {
		select {
		case n.membershipEvents <- struct{}{}:
		default:
		}
	}
}

// MembershipEvents signals an applied configuration or a newly elected leader.
// The receiver reads Membership for the latest complete snapshot.
func (n *Instance) MembershipEvents() <-chan struct{} { return n.membershipEvents }

// Run blocks until ctx is cancelled, Close is called or a fatal apply error occurs.
// It must be called once. All send workers are joined before Run returns.
func (n *Instance) Run(ctx context.Context) (runErr error) {
	n.mu.Lock()
	if n.lifecycle != 0 {
		n.mu.Unlock()
		return fmt.Errorf("instance can only be run once")
	}
	n.lifecycle = 1
	n.mu.Unlock()
	workerCtx, cancel := context.WithCancel(ctx)
	n.workerCtx = workerCtx
	for peer, queue := range n.outbound {
		n.startSender(peer, queue)
	}
	defer func() {
		message := ""
		if runErr != nil {
			message = runErr.Error()
		}
		n.publish(false, message)
		cancel()
		n.workers.Wait()
		for id, p := range n.pending {
			p.reply <- result{err: unavailable("instance stopped; proposed writes may have committed")}
			delete(n.pending, id)
		}
		n.mu.Lock()
		n.lifecycle = 2
		close(n.done)
		n.mu.Unlock()
	}()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		if err := n.processReady(); err != nil {
			return err
		}
		n.completeReads()
		n.publish(true, "")
		select {
		case <-ctx.Done():
			return nil
		case <-n.stop:
			return nil
		case <-ticker.C:
			n.raw.Tick()
			n.expireRequests()
		case m := <-n.incoming:
			// Membership and local-message checks happen in Step. A stale network
			// message rejected by the protocol does not fail the instance.
			if !n.known.Load().(map[uint64]bool)[m.From] {
				continue
			}
			if err := n.raw.Step(m); err != nil && err != raft.ErrStepPeerNotFound && err != raft.ErrProposalDropped {
				return err
			}
		case d := <-n.deliveries:
			if d.err != nil {
				n.raw.ReportUnreachable(d.to)
			}
			if d.snapshot {
				state := raft.SnapshotFinish
				if d.err != nil {
					state = raft.SnapshotFailure
				}
				n.raw.ReportSnapshot(d.to, state)
			}
		case r := <-n.requests:
			n.accept(r)
		}
	}
}

// Close is idempotent, also before Run. It waits for the owner loop and workers.
func (n *Instance) Close() error {
	n.stopOnce.Do(func() { close(n.stop) })
	n.mu.Lock()
	if n.lifecycle == 0 {
		n.lifecycle = 2
		close(n.done)
	}
	n.mu.Unlock()
	<-n.done
	return nil
}

// Step is the receiving transport boundary. It accepts only messages addressed
// to this peer from configured peers in this group, never local control messages.
func (n *Instance) Step(ctx context.Context, group string, data []byte) error {
	if group != n.cfg.GroupID {
		return status.Error(codes.InvalidArgument, "wrong Raft group")
	}
	if int64(len(data)) > n.cfg.MaxWireBytes {
		return status.Error(codes.ResourceExhausted, "Raft message too large")
	}
	var m raftpb.Message
	if err := m.Unmarshal(data); err != nil {
		return status.Error(codes.InvalidArgument, "invalid Raft message")
	}
	known := n.known.Load().(map[uint64]bool)[m.From]
	if !known || m.From == n.cfg.PeerID || m.To != n.cfg.PeerID || raft.IsLocalMsg(m.Type) || m.Type == raftpb.MsgProp || m.Type == raftpb.MsgReadIndex {
		return status.Error(codes.InvalidArgument, "invalid Raft message source, target or type")
	}
	if !n.Status().Running {
		return unavailable("instance is not running")
	}
	select {
	case n.incoming <- m:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-n.stop:
		return unavailable("instance stopping")
	case <-n.done:
		return unavailable("instance stopped")
	}
}

func (n *Instance) sendLoop(ctx context.Context, peer uint64, queue <-chan outgoing) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-queue:
			attempt, cancel := context.WithTimeout(ctx, n.cfg.SendTimeout)
			err := n.transport.Send(attempt, n.cfg.GroupID, peer, m.data)
			cancel()
			if err != nil && ctx.Err() == nil && n.cfg.Observer != nil {
				n.cfg.Observer.RecordSendError()
			}
			select {
			case n.deliveries <- delivery{to: peer, snapshot: m.snapshot, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *Instance) accept(r request) {
	if err := r.ctx.Err(); err != nil {
		r.reply <- result{err: status.FromContextError(err).Err()}
		return
	}
	if len(n.pending) >= n.cfg.MaxPending {
		r.reply <- result{err: status.Error(codes.ResourceExhausted, "too many pending requests")}
		return
	}
	s := n.raw.BasicStatus()
	if !n.canServe() {
		r.reply <- result{err: unavailable("member is joining, a learner, or removed")}
		return
	}
	read := r.op == opGet || r.op == opBatchGet || r.op == opReadOutbox
	if read && r.op != opReadOutbox && n.cfg.ReadMode == Local {
		if s.Lead == 0 || n.applied < s.Commit {
			r.reply <- result{err: unavailable("replica is not ready")}
			return
		}
		message, err := decodeRequest(r.op, r.data)
		if err != nil {
			r.reply <- result{err: err}
			return
		}
		response, err := n.execute(message)
		r.reply <- result{message: response, err: err}
		return
	}
	if s.RaftState != raft.StateLeader {
		r.reply <- result{err: &NotLeaderError{LeaderID: s.Lead, Address: n.peers[s.Lead].Address, Term: s.Term}}
		return
	}
	if n.sequence == math.MaxUint64 {
		r.reply <- result{err: status.Error(codes.ResourceExhausted, "proposal sequence exhausted")}
		return
	}
	n.sequence++
	var id requestID
	binary.BigEndian.PutUint64(id[:8], n.cfg.PeerID)
	binary.BigEndian.PutUint64(id[8:], n.sequence)
	n.pending[id] = &pendingRequest{request: r}
	if read || r.change != nil {
		n.pending[id].readStarted = time.Now()
		n.raw.ReadIndex(id[:])
	} else if err := n.raw.Propose(encodeCommand(r.op, id, r.data)); err != nil {
		delete(n.pending, id)
		r.reply <- result{err: status.Error(codes.ResourceExhausted, "Raft rejected proposal")}
	}
}

func (n *Instance) expireRequests() {
	for id, p := range n.pending {
		if err := p.ctx.Err(); err != nil {
			p.reply <- result{err: status.FromContextError(err).Err()}
			delete(n.pending, id)
		}
	}
}

func (n *Instance) completeReads() {
	for id, p := range n.pending {
		if p.change != nil && n.completeMembership(id, p) {
			continue
		}
		if !p.readReady || p.readIndex > n.applied {
			continue
		}
		if p.change != nil {
			n.proposeMembership(id, p)
			continue
		}
		message, err := decodeRequest(p.op, p.data)
		var response proto.Message
		if err == nil {
			response, err = n.execute(message)
		}
		p.reply <- result{message: response, err: err}
		delete(n.pending, id)
	}
}

func (n *Instance) observe(stage TimingStage, start time.Time) {
	if n.cfg.Observer != nil {
		n.cfg.Observer.ObserveDuration(stage, time.Since(start))
	}
}

func (n *Instance) processReady() error {
	for n.raw.HasReady() {
		rd := n.raw.Ready()
		var readStateAt time.Time
		if len(rd.ReadStates) > 0 {
			readStateAt = time.Now()
		}
		if !raft.IsEmptySnap(rd.Snapshot) {
			n.observed.Store(n.snapshot(true, ""))
			started := time.Now()
			err := n.restoreSnapshot(rd.Snapshot.Data)
			n.observe(StageRestore, started)
			if err != nil {
				return fmt.Errorf("restore checkpoint: %w", err)
			}
			if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
				return err
			}
			n.applied, n.snapshotIndex, n.lastIndex = rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Index
			n.conf = rd.Snapshot.Metadata.ConfState
			n.publishKnown()
		}
		if err := n.storage.Append(rd.Entries); err != nil {
			return err
		}
		if len(rd.Entries) > 0 {
			n.lastIndex = rd.Entries[len(rd.Entries)-1].Index
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := n.storage.SetHardState(rd.HardState); err != nil {
				return err
			}
		}
		if len(rd.CommittedEntries) > 0 {
			n.observed.Store(n.snapshot(true, ""))
		}
		// Marshal before handing data to workers; RawNode messages must not be
		// marshalled concurrently with subsequent Ready/Advance calls.
		for _, m := range rd.Messages {
			data, err := m.Marshal()
			if err != nil {
				return err
			}
			queue, ok := n.outbound[m.To]
			if !ok {
				// A joining member may not yet have applied the address record.
				n.raw.ReportUnreachable(m.To)
				continue
			}
			select {
			case queue <- outgoing{data: data, snapshot: m.Type == raftpb.MsgSnap}:
			default:
				n.raw.ReportUnreachable(m.To)
				if m.Type == raftpb.MsgSnap {
					n.raw.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
			}
		}
		for _, entry := range rd.CommittedEntries {
			if entry.Index <= n.applied {
				continue
			}
			if entry.Index != n.applied+1 {
				return fmt.Errorf("gap in committed entries")
			}
			switch entry.Type {
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				n.conf = *n.raw.ApplyConfChange(cc)
				if len(cc.Context) != 0 {
					var peer Peer
					if err := json.Unmarshal(cc.Context, &peer); err != nil {
						return err
					}
					if err := n.installPeer(peer); err != nil {
						return err
					}
				}
				n.configIndex = entry.Index
				n.publishKnown()
			case raftpb.EntryConfChangeV2:
				if err := n.applyMembership(entry); err != nil {
					return err
				}
			case raftpb.EntryNormal:
				if len(entry.Data) > 0 {
					op, id, payload, err := decodeCommand(entry.Data)
					if err != nil {
						return err
					}
					message, err := decodeRequest(op, payload)
					if err != nil {
						return fmt.Errorf("decode committed request: %w", err)
					}
					started := time.Now()
					response, err := n.execute(message)
					n.observe(StageApply, started)
					if !expectedApplicationError(err) {
						return fmt.Errorf("apply entry %d: %w", entry.Index, err)
					}
					if p, ok := n.pending[id]; ok {
						p.reply <- result{message: response, err: err}
						delete(n.pending, id)
					}
				}
			default:
				return fmt.Errorf("unsupported membership entry")
			}
			n.applied = entry.Index
		}
		for _, state := range rd.ReadStates {
			if len(state.RequestCtx) != len(requestID{}) {
				return fmt.Errorf("invalid read correlation ID")
			}
			var id requestID
			copy(id[:], state.RequestCtx)
			if p := n.pending[id]; p != nil {
				if !p.readReady && n.cfg.Observer != nil {
					n.cfg.Observer.ObserveDuration(StageReadIndex, readStateAt.Sub(p.readStarted))
				}
				p.readReady, p.readIndex = true, state.Index
			}
		}
		n.raw.Advance(rd)
		// A new member rejects a snapshot whose ConfState does not include it.
		// Once compaction has happened, refresh the snapshot on every membership
		// change instead of waiting for another SnapshotEntries writes.
		if n.applied-n.snapshotIndex >= n.cfg.SnapshotEntries || (n.snapshotIndex > 0 && n.configIndex > n.snapshotIndex) {
			started := time.Now()
			data, err := n.makeSnapshot()
			n.observe(StageCheckpoint, started)
			if err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			if int64(len(data)) > n.cfg.MaxWireBytes-wireOverhead {
				return fmt.Errorf("checkpoint exceeds PoC transport limit")
			}
			if _, err := n.storage.CreateSnapshot(n.applied, &n.conf, data); err != nil {
				return err
			}
			if err := n.storage.Compact(n.applied); err != nil {
				return err
			}
			n.snapshotIndex = n.applied
		}
	}
	return nil
}
