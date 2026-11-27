package cpapi

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/statestore"
)

type targetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

type scrapeEndpoint struct {
	address string
	scheme  string
}

type discoveryNode struct {
	region string
	id     string
}

type discoveryGroup struct {
	region    string
	partition uint32
}

// StorageDiscovery serves a complete Prometheus HTTP SD snapshot. An incomplete
// scope returns 503 so Prometheus keeps its previous targets.
func (s *Server) StorageDiscovery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	query, err := url.ParseQuery(r.URL.RawQuery)
	regions, filtered := query["region"]
	if err != nil || (filtered && (len(regions) != 1 || regions[0] == "" || len(regions[0]) > MaxIDBytes)) {
		http.Error(w, "expected at most one non-empty region", http.StatusBadRequest)
		return
	}
	region := query.Get("region")
	view, err := s.discoverySnapshot(r.Context())
	if err != nil {
		s.log.ErrorContext(r.Context(), "read storage discovery snapshot", "error", err)
		http.Error(w, "storage discovery unavailable", http.StatusServiceUnavailable)
		return
	}
	if view.Config == nil {
		http.Error(w, "storage discovery unavailable", http.StatusServiceUnavailable)
		return
	}
	if filtered && !slices.ContainsFunc(view.Config.Regions, func(r cpconfig.Region) bool { return r.ID == region }) {
		http.Error(w, "unknown region", http.StatusNotFound)
		return
	}
	targets, err := discoveryTargets(view, region)
	if err != nil {
		s.log.DebugContext(r.Context(), "storage discovery incomplete", "region", region, "error", err)
		http.Error(w, "storage discovery unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(targets); err != nil {
		s.log.DebugContext(r.Context(), "write storage discovery", "error", err)
	}
}

func (s *Server) discoverySnapshot(ctx context.Context) (statestore.Snapshot, error) {
	return s.store.LiveConfig(ctx, s.clock.Now().UTC(), s.nodeTTL)
}

func discoveryTargets(view statestore.Snapshot, region string) ([]targetGroup, error) {
	config := view.Config
	groups := make(map[discoveryGroup]cpconfig.GroupMembership, len(view.AppliedMemberships))
	for _, m := range view.AppliedMemberships {
		groups[discoveryGroup{m.Region, m.PartitionID}] = m
	}
	endpoints := make(map[discoveryNode]scrapeEndpoint)
	owners := make(map[scrapeEndpoint]discoveryNode)
	add := func(node discoveryNode, address string) error {
		endpoint, err := storageScrapeEndpoint(address)
		if err != nil {
			return fmt.Errorf("node %q: %w", node.id, err)
		}
		if other, ok := endpoints[node]; ok && other != endpoint {
			return fmt.Errorf("node %q has conflicting Raft addresses", node.id)
		}
		if other, ok := owners[endpoint]; ok && other != node {
			return fmt.Errorf("nodes %q and %q share a Raft endpoint", other.id, node.id)
		}
		endpoints[node], owners[endpoint] = endpoint, node
		return nil
	}

	for _, a := range config.Partitions {
		if region != "" && a.Region != region {
			continue
		}
		if m, ok := groups[discoveryGroup{a.Region, a.PartitionID}]; ok {
			for _, member := range m.Members {
				if err := add(discoveryNode{a.Region, member.NodeID}, member.RaftAddress); err != nil {
					return nil, err
				}
			}
			continue
		}
		if !view.BootstrapDiscovery {
			return nil, fmt.Errorf("awaiting applied membership for %s/%d", a.Region, a.PartitionID)
		}
		for _, id := range a.ReplicaNodeIDs {
			node, _ := config.NodeByID(id)
			if err := add(discoveryNode{a.Region, id}, node.RaftAddress); err != nil {
				return nil, err
			}
		}
	}
	nodes := make([]discoveryNode, 0, len(endpoints))
	for node := range endpoints {
		nodes = append(nodes, node)
	}
	slices.SortFunc(nodes, func(a, b discoveryNode) int {
		if c := cmp.Compare(a.region, b.region); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	out := make([]targetGroup, 0, len(nodes))
	for _, node := range nodes {
		endpoint := endpoints[node]
		labels := map[string]string{"node_id": node.id, "region": node.region}
		if endpoint.scheme == "https" {
			labels["__scheme__"] = endpoint.scheme
		}
		out = append(out, targetGroup{Targets: []string{endpoint.address}, Labels: labels})
	}
	return out, nil
}

func storageScrapeEndpoint(address string) (scrapeEndpoint, error) {
	u, err := url.Parse(address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || strings.Contains(address, "#") {
		return scrapeEndpoint{}, errors.New("expected an HTTP(S) Raft URL without credentials or fragment")
	}
	host, port := u.Hostname(), u.Port()
	if port == "" && !strings.HasSuffix(u.Host, ":") {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return scrapeEndpoint{}, errors.New("Raft URL port must be in 1..65535")
	}
	bracketed := strings.HasPrefix(u.Host, "[")
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Is6() != bracketed {
			return scrapeEndpoint{}, errors.New("Raft URL has invalid IP literal brackets")
		}
		ip = ip.Unmap()
		if ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
			return scrapeEndpoint{}, errors.New("Raft URL requires a routable host")
		}
		host = ip.String()
	} else if bracketed || !validDNSName(host) {
		return scrapeEndpoint{}, errors.New("Raft URL requires an IP address or DNS name")
	}
	return scrapeEndpoint{address: net.JoinHostPort(strings.ToLower(host), strconv.FormatUint(portNumber, 10)), scheme: u.Scheme}, nil
}

func validDNSName(host string) bool {
	const maxDNSName, maxDNSLabel = 253, 63
	host = strings.TrimSuffix(host, ".")
	if len(host) > maxDNSName {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if len(label) == 0 || len(label) > maxDNSLabel || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func (s *Server) raftMembers(ctx context.Context) ([]cpmetrics.GaugeSample, error) {
	view, err := s.discoverySnapshot(ctx)
	if err != nil || view.Config == nil {
		return nil, err
	}
	var samples []cpmetrics.GaugeSample
	for _, m := range view.AppliedMemberships {
		for _, member := range m.Members {
			samples = append(samples, cpmetrics.GaugeSample{
				Labels: map[string]string{"region": m.Region, "partition": strconv.FormatUint(uint64(m.PartitionID), 10), "node_id": member.NodeID},
				Value:  1,
			})
		}
	}
	return samples, nil
}

func (s *Server) raftMembershipFreshness(ctx context.Context) ([]cpmetrics.GaugeSample, error) {
	view, err := s.discoverySnapshot(ctx)
	if err != nil || view.Config == nil {
		return nil, err
	}
	status := make(map[discoveryGroup]float64, len(view.AppliedMemberships))
	for _, m := range view.AppliedMemberships {
		value := float64(0)
		if m.Confirmed {
			value = 1
		}
		status[discoveryGroup{m.Region, m.PartitionID}] = value
	}
	samples := make([]cpmetrics.GaugeSample, 0, len(view.Config.Partitions))
	for _, a := range view.Config.Partitions {
		value, known := status[discoveryGroup{a.Region, a.PartitionID}]
		if !known {
			value = -1
		}
		samples = append(samples, cpmetrics.GaugeSample{
			Labels: map[string]string{"region": a.Region, "partition": strconv.FormatUint(uint64(a.PartitionID), 10)},
			Value:  value,
		})
	}
	return samples, nil
}
