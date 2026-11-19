package gateway

import (
	"fmt"
	"slices"
)

// Preferences are scoped to the region of the Raft group, not the Gateway.
type forwardRoute struct {
	partition uint32
	region    string
}

type routeToken struct {
	revision    uint64
	leader      Leader
	observation leaderObservation
}

type preference struct {
	address string
	token   routeToken
}

func (r *Router) forwardingRoute(partition uint32, write bool) (partitionRoute, routeToken, error) {
	s := r.cur.Load()
	if s == nil {
		return partitionRoute{}, routeToken{}, ErrNoConfig
	}
	if partition >= s.partitionCount {
		return partitionRoute{}, routeToken{}, fmt.Errorf("%w: %d", ErrPartitionOutOfRange, partition)
	}
	routes := s.local
	if write {
		routes = s.write
	}
	route := routes[partition]
	if len(route.replicas) == 0 {
		return partitionRoute{}, routeToken{}, fmt.Errorf("%w: no ready replicas for partition %d", ErrNoLeader, partition)
	}
	return route, routeToken{s.revision, route.leader, route.observation}, nil
}

func (route partitionRoute) contains(address string) bool {
	return slices.ContainsFunc(route.replicas, func(replica Leader) bool { return replica.Address == address })
}
