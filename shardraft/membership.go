package shardraft

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"unicode/utf8"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MembershipChange is an operator intent, serialized in ConfChangeV2.Context.
// OperationID must be reused, with the identical payload, after an uncertain
// response. Add uses Peer; other actions use PeerID. Replace also uses RemoveID.
type MembershipChange struct {
	OperationID string `json:"operation_id"`
	Action      string `json:"action"` // add-learner, promote, remove, replace
	Peer        Peer   `json:"peer,omitempty"`
	PeerID      uint64 `json:"peer_id,omitempty"`
	RemoveID    uint64 `json:"remove_id,omitempty"`
}

type Member struct {
	Peer
	Voter         bool `json:"voter"`
	Learner       bool `json:"learner"`
	OutgoingVoter bool `json:"outgoing_voter"`
}

// Membership is a local observation, not a linearizable query. Index identifies
// the last applied configuration entry. Joint means replacement is still in
// progress. A successful ChangeMembership waits for the final configuration.
type Membership struct {
	GroupID    string   `json:"group_id"`
	LeaderID   uint64   `json:"leader_id"`
	Term       uint64   `json:"term"`
	Index      uint64   `json:"index"`
	Joint      bool     `json:"joint"`
	Members    []Member `json:"members"`
	RemovedIDs []uint64 `json:"removed_ids"`
}

type MembershipResult struct {
	OperationID string     `json:"operation_id"`
	ChangeIndex uint64     `json:"change_index"`
	Membership  Membership `json:"membership"`
}

type membershipRecord struct {
	Change MembershipChange
	Index  uint64
}

// Bounds keep metadata and retry history within the in-memory snapshot budget.
const maxKnownPeers = 128
const maxMembershipOperations = 1024

// AddLearner adds a non-voting member. Success confirms membership, not catch-up.
// The learner receives replicated data but cannot serve cache requests.
func (n *Instance) AddLearner(ctx context.Context, operationID string, peer Peer) (*MembershipResult, error) {
	return n.ChangeMembership(ctx, MembershipChange{OperationID: operationID, Action: "add-learner", Peer: peer})
}

// PromoteLearner promotes an existing caught-up learner. If it is not caught
// up yet, it returns FailedPrecondition without scheduling an automatic retry.
func (n *Instance) PromoteLearner(ctx context.Context, operationID string, peerID uint64) (*MembershipResult, error) {
	return n.ChangeMembership(ctx, MembershipChange{OperationID: operationID, Action: "promote", PeerID: peerID})
}

// RemoveNode removes a voter or learner. Removing the current leader is not
// supported. The remaining nodes must still be able to commit the transition.
func (n *Instance) RemoveNode(ctx context.Context, operationID string, peerID uint64) (*MembershipResult, error) {
	return n.ChangeMembership(ctx, MembershipChange{OperationID: operationID, Action: "remove", PeerID: peerID})
}

// Replace promotes an existing caught-up learner and removes oldPeerID in one
// joint-consensus transition. It neither adds a learner nor waits for catch-up:
// a missing or lagging learner returns FailedPrecondition before proposal.
func (n *Instance) Replace(ctx context.Context, operationID string, oldPeerID, learnerID uint64) (*MembershipResult, error) {
	return n.ChangeMembership(ctx, MembershipChange{OperationID: operationID, Action: "replace", PeerID: learnerID, RemoveID: oldPeerID})
}

// ChangeMembership requires a leader and quorum. A timeout does not roll back
// a proposal. Only one learner and one configuration transition are allowed at
// a time. Removing the current leader is deliberately excluded from this MVP.
func (n *Instance) ChangeMembership(ctx context.Context, change MembershipChange) (*MembershipResult, error) {
	if err := validateMembershipChange(change); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	if !n.Status().Running {
		return nil, unavailable("instance is not running")
	}
	r := request{ctx: ctx, change: &change, reply: make(chan result, 1)}
	select {
	case n.requests <- r:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-n.done:
		return nil, unavailable("instance stopped")
	case <-n.stop:
		return nil, unavailable("instance stopping")
	}
	select {
	case out := <-r.reply:
		return out.membership, out.err
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-n.done:
		return nil, unavailable("instance stopped; membership change may have committed")
	}
}

func validateMembershipChange(c MembershipChange) error {
	bad := func(message string) error { return status.Error(codes.InvalidArgument, message) }
	validID := func(id uint64) bool { return id != 0 && !raft.IsLocalMsgTarget(id) }
	if len(c.OperationID) == 0 || len(c.OperationID) > 128 {
		return bad("operation_id must contain 1..128 bytes")
	}
	for _, value := range []string{c.OperationID, c.Peer.Address, c.Peer.NodeID, c.Peer.RaftAddress} {
		if !utf8.ValidString(value) {
			return bad("membership strings must be valid UTF-8")
		}
	}
	data, _ := json.Marshal(c)
	if len(data) > 4096 {
		return bad("membership command exceeds 4096 bytes")
	}
	switch c.Action {
	case "add-learner":
		if !validID(c.Peer.ID) || c.PeerID != 0 || c.RemoveID != 0 {
			return bad("add-learner requires only peer")
		}
	case "promote", "remove":
		if !validID(c.PeerID) || c.Peer != (Peer{}) || c.RemoveID != 0 {
			return bad("promote/remove require only peer_id")
		}
	case "replace":
		if !validID(c.PeerID) || !validID(c.RemoveID) || c.PeerID == c.RemoveID || c.Peer != (Peer{}) {
			return bad("replace requires distinct peer_id (learner) and remove_id")
		}
	default:
		return bad("unknown membership action")
	}
	return nil
}

func (n *Instance) Membership() Membership {
	m := n.membership.Load().(Membership)
	m.Members = slices.Clone(m.Members)
	m.RemovedIDs = slices.Clone(m.RemovedIDs)
	return m
}

func (n *Instance) membershipView() Membership {
	s := n.raw.BasicStatus()
	m := Membership{GroupID: n.cfg.GroupID, LeaderID: s.Lead, Term: s.Term, Index: n.configIndex, Joint: len(n.conf.VotersOutgoing) > 0}
	for id, peer := range n.peers {
		voter, learner, outgoing := slices.Contains(n.conf.Voters, id), n.isLearner(id), slices.Contains(n.conf.VotersOutgoing, id)
		if voter || learner || outgoing {
			m.Members = append(m.Members, Member{Peer: peer, Voter: voter, Learner: learner, OutgoingVoter: outgoing})
		}
		if n.removed[id] {
			m.RemovedIDs = append(m.RemovedIDs, id)
		}
	}
	sort.Slice(m.Members, func(i, j int) bool { return m.Members[i].ID < m.Members[j].ID })
	slices.Sort(m.RemovedIDs)
	return m
}

func (n *Instance) isLearner(id uint64) bool {
	return slices.Contains(n.conf.Learners, id) || slices.Contains(n.conf.LearnersNext, id)
}
func (n *Instance) isVoter(id uint64) bool {
	return slices.Contains(n.conf.Voters, id) || slices.Contains(n.conf.VotersOutgoing, id)
}
func (n *Instance) canServe() bool { return n.isVoter(n.cfg.PeerID) && !n.removed[n.cfg.PeerID] }

func (n *Instance) publishKnown() {
	known := make(map[uint64]bool, len(n.peers))
	for id := range n.peers {
		known[id] = !n.removed[id] || slices.Contains(n.conf.VotersOutgoing, id)
	}
	n.known.Store(known)
}

func (n *Instance) startSender(id uint64, queue <-chan outgoing) {
	n.workers.Add(1)
	go func() { defer n.workers.Done(); n.sendLoop(n.workerCtx, id, queue) }()
}

func (n *Instance) installPeer(peer Peer) error {
	if transport, ok := n.transport.(MembershipTransport); ok {
		if err := transport.SetPeer(peer); err != nil {
			return err
		}
	}
	n.peers[peer.ID] = peer
	if peer.ID != n.cfg.PeerID && n.outbound[peer.ID] == nil {
		queue := make(chan outgoing, 8)
		n.outbound[peer.ID] = queue
		n.startSender(peer.ID, queue)
	}
	return nil
}

// This is called only after a quorum ReadIndex barrier was applied locally.
func (n *Instance) proposeMembership(id requestID, p *pendingRequest) {
	p.readReady, p.adminVerified = false, true
	c := *p.change
	s := n.raw.BasicStatus()
	fail := func(err error) { p.reply <- result{err: err}; delete(n.pending, id) }
	if err := p.ctx.Err(); err != nil {
		fail(status.FromContextError(err).Err())
		return
	}
	if s.RaftState != raft.StateLeader {
		fail(&NotLeaderError{LeaderID: s.Lead, Address: n.peers[s.Lead].Address, Term: s.Term})
		return
	}
	if n.completeMembership(id, p) {
		return
	}
	if n.proposalTerm != s.Term {
		n.proposing = nil
	}
	if n.proposing != nil {
		if *n.proposing == c {
			return
		}
		fail(unavailable("another membership change is pending; retry later"))
		return
	}
	if len(n.conf.VotersOutgoing) != 0 {
		fail(unavailable("joint configuration is still in progress"))
		return
	}
	if len(n.operations) >= maxMembershipOperations {
		fail(status.Error(codes.ResourceExhausted, "membership operation history is full"))
		return
	}
	cc, err := n.configurationChange(c)
	if err != nil {
		fail(err)
		return
	}
	cc.Context, _ = json.Marshal(c)
	if err := n.raw.ProposeConfChange(cc); err != nil {
		fail(unavailable("Raft rejected membership proposal"))
		return
	}
	n.proposing, n.proposalTerm = &c, s.Term
}

func (n *Instance) completeMembership(id requestID, p *pendingRequest) bool {
	if !p.adminVerified {
		return false
	}
	record, ok := n.operations[p.change.OperationID]
	if !ok {
		return false
	}
	if record.Change != *p.change {
		p.reply <- result{err: status.Error(codes.AlreadyExists, "operation_id was used for a different change")}
	} else {
		if len(n.conf.VotersOutgoing) != 0 {
			// Wait for auto-leave, keeping the waiter until the final config.
			return true
		}
		p.reply <- result{membership: &MembershipResult{OperationID: p.change.OperationID, ChangeIndex: record.Index, Membership: n.membershipView()}}
	}
	delete(n.pending, id)
	return true
}

func (n *Instance) configurationChange(c MembershipChange) (raftpb.ConfChangeV2, error) {
	var cc raftpb.ConfChangeV2
	bad := func(message string) (raftpb.ConfChangeV2, error) {
		return cc, status.Error(codes.FailedPrecondition, message)
	}
	add := func(kind raftpb.ConfChangeType, id uint64) {
		cc.Changes = append(cc.Changes, raftpb.ConfChangeSingle{Type: kind, NodeID: id})
	}
	switch c.Action {
	case "add-learner":
		if _, used := n.peers[c.Peer.ID]; used {
			return bad("peer ID was already used; choose a new identity")
		}
		if len(n.peers) >= maxKnownPeers {
			return bad("peer identity history is full")
		}
		if len(n.conf.Learners) != 0 {
			return bad("only one learner is allowed")
		}
		transport, ok := n.transport.(MembershipTransport)
		if !ok {
			return bad("transport does not support dynamic membership")
		}
		if err := transport.ValidatePeer(c.Peer); err != nil {
			return cc, status.Error(codes.InvalidArgument, err.Error())
		}
		for id, peer := range n.peers {
			if !n.removed[id] && ((c.Peer.RaftAddress != "" && peer.RaftAddress == c.Peer.RaftAddress) || (c.Peer.Address != "" && peer.Address == c.Peer.Address) || (c.Peer.NodeID != "" && peer.NodeID == c.Peer.NodeID)) {
				return bad("active peer already uses this address or node identity")
			}
		}
		add(raftpb.ConfChangeAddLearnerNode, c.Peer.ID)
	case "promote", "replace":
		if !n.isLearner(c.PeerID) {
			return bad("target must be a learner")
		}
		s := n.raw.Status()
		progress, ok := s.Progress[c.PeerID]
		if !ok || !progress.RecentActive || progress.Match < s.Commit {
			return bad("learner has not caught up; retry promotion later")
		}
		add(raftpb.ConfChangeAddNode, c.PeerID)
		if c.Action == "replace" {
			if !slices.Contains(n.conf.Voters, c.RemoveID) || c.RemoveID == n.cfg.PeerID {
				return bad("replace requires an existing voter other than the leader")
			}
			add(raftpb.ConfChangeRemoveNode, c.RemoveID)
			cc.Transition = raftpb.ConfChangeTransitionJointImplicit
		}
	case "remove":
		if !n.isVoter(c.PeerID) && !n.isLearner(c.PeerID) {
			return bad("target is not a member")
		}
		if c.PeerID == n.cfg.PeerID {
			return bad("cannot remove current leader")
		}
		add(raftpb.ConfChangeRemoveNode, c.PeerID)
		// Keep the old member in the outgoing set during the transition, giving
		// a reachable removed replica the committed fence before final removal.
		cc.Transition = raftpb.ConfChangeTransitionJointImplicit
	}
	return cc, nil
}

func (n *Instance) applyMembership(entry raftpb.Entry) error {
	var cc raftpb.ConfChangeV2
	if err := cc.Unmarshal(entry.Data); err != nil {
		return err
	}
	if len(cc.Context) != 0 {
		var c MembershipChange
		if err := json.Unmarshal(cc.Context, &c); err != nil {
			return err
		}
		if err := validateMembershipChange(c); err != nil {
			return err
		}
		if _, exists := n.operations[c.OperationID]; exists {
			return fmt.Errorf("duplicate committed membership operation %q", c.OperationID)
		}
		if c.Action == "add-learner" {
			if err := n.installPeer(c.Peer); err != nil {
				return err
			}
		}
		if c.Action == "remove" {
			n.removed[c.PeerID] = true
		}
		if c.Action == "replace" {
			n.removed[c.RemoveID] = true
		}
		n.operations[c.OperationID] = membershipRecord{Change: c, Index: entry.Index}
	}
	n.conf = *n.raw.ApplyConfChange(cc)
	n.configIndex, n.proposing = entry.Index, nil
	n.publishKnown()
	return nil
}
