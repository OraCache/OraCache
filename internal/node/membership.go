package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaxStoreBytes bounds JSON checkpoints while the MVP sends snapshots in one
// HTTP message. Store's standalone default is intentionally unaffected.
const MaxStoreBytes int64 = 8 << 20

func (n *Node) Membership() shardraft.Membership { return n.inst.Membership() }

// These application entry points retain the same placement guard as the HTTP
// API. Do not bypass it by calling the underlying Instance directly.
func (n *Node) AddLearner(ctx context.Context, operationID string, peer shardraft.Peer) (*shardraft.MembershipResult, error) {
	return n.ChangeMembership(ctx, shardraft.MembershipChange{OperationID: operationID, Action: "add-learner", Peer: peer})
}

func (n *Node) PromoteLearner(ctx context.Context, operationID string, peerID uint64) (*shardraft.MembershipResult, error) {
	return n.ChangeMembership(ctx, shardraft.MembershipChange{OperationID: operationID, Action: "promote", PeerID: peerID})
}

func (n *Node) RemoveNode(ctx context.Context, operationID string, peerID uint64) (*shardraft.MembershipResult, error) {
	return n.ChangeMembership(ctx, shardraft.MembershipChange{OperationID: operationID, Action: "remove", PeerID: peerID})
}

// Replace promotes an existing caught-up learner and removes the old voter.
// It retains the application's placement guard, just like the HTTP endpoint.
func (n *Node) Replace(ctx context.Context, operationID string, oldPeerID, learnerID uint64) (*shardraft.MembershipResult, error) {
	return n.ChangeMembership(ctx, shardraft.MembershipChange{OperationID: operationID, Action: "replace", PeerID: learnerID, RemoveID: oldPeerID})
}

func (n *Node) ChangeMembership(ctx context.Context, c shardraft.MembershipChange) (*shardraft.MembershipResult, error) {
	if c.Action == "add-learner" && (c.Peer.NodeID == "" || c.Peer.Address == "" || c.Peer.RaftAddress == "") {
		return nil, status.Error(codes.InvalidArgument, "peer needs node_id, address and raft_address")
	}
	return n.inst.ChangeMembership(ctx, c)
}

// Uses the existing private Raft listener. Like /raft this MVP endpoint has no
// authentication; it must not be exposed to untrusted clients.
func (n *Node) membershipHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(n.Membership())
	case http.MethodPost:
		var body struct {
			GroupID string `json:"group_id"`
			shardraft.MembershipChange
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			n.membershipHTTPError(w, status.Error(codes.InvalidArgument, "invalid membership JSON"))
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF || body.GroupID != n.cfg.GroupID {
			n.membershipHTTPError(w, status.Error(codes.InvalidArgument, "expected one request for this group_id"))
			return
		}
		result, err := n.ChangeMembership(r.Context(), body.MembershipChange)
		if err != nil {
			n.membershipHTTPError(w, err)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	default:
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (n *Node) membershipHTTPError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpCode := http.StatusInternalServerError
	switch code {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.FailedPrecondition, codes.AlreadyExists:
		httpCode = http.StatusConflict
	case codes.ResourceExhausted:
		httpCode = http.StatusTooManyRequests
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		httpCode = http.StatusGatewayTimeout
	case codes.Canceled:
		httpCode = http.StatusRequestTimeout
	}
	response := map[string]any{"error": err.Error(), "code": code.String()}
	var hint *shardraft.NotLeaderError
	if errors.As(err, &hint) {
		response["leader_id"], response["leader_address"], response["term"] = hint.LeaderID, hint.Address, hint.Term
	}
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(response)
}
