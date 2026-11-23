package cplink

import (
	"context"
	"net/url"
	"strings"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Authorize pins node heartbeat permissions to an authenticated URI SAN.
func Authorize(cluster string, mutualTLS bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, call grpc.UnaryHandler) (any, error) {
		if cluster != "" {
			values := metadata.ValueFromIncomingContext(ctx, ClusterHeader)
			if len(values) != 1 || values[0] != cluster {
				return nil, status.Error(codes.FailedPrecondition, "unexpected Control Plane cluster")
			}
			if err := grpc.SetHeader(ctx, metadata.Pairs(ClusterHeader, cluster)); err != nil {
				return nil, status.Error(codes.Internal, "could not identify Control Plane cluster")
			}
		}
		if mutualTLS && !authorizedClient(ctx, req, cluster) {
			return nil, status.Error(codes.PermissionDenied, "Control Plane client identity does not authorize this request")
		}
		return call(ctx, req)
	}
}

func authorizedClient(ctx context.Context, req any, cluster string) bool {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	auth, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(auth.State.VerifiedChains) == 0 || len(auth.State.PeerCertificates) == 0 {
		return false
	}
	for _, uri := range auth.State.PeerCertificates[0].URIs {
		kind, region, node, ok := clientIdentity(uri, cluster)
		if !ok {
			continue
		}
		switch req := req.(type) {
		case *controlplanev1.HeartbeatRequest:
			if kind == "node" && node == req.GetNodeId() && region == req.GetRegion() {
				return true
			}
		case *controlplanev1.GetConfigRequest:
			if node == req.GetNodeId() && (kind == "node" && req.GetKind() == controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE ||
				kind == "gateway" && req.GetKind() == controlplanev1.AgentKind_AGENT_KIND_GATEWAY) {
				return true
			}
		}
	}
	return false
}

func clientIdentity(uri *url.URL, cluster string) (kind, region, node string, valid bool) {
	if uri.Scheme != "spiffe" || uri.Host != cluster || uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" || uri.RawPath != "" {
		return "", "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(uri.Path, "/"), "/")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}
