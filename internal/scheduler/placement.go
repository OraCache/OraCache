package scheduler

import (
	"slices"

	"github.com/OraCache/oracache/internal/cpconfig"
)

type placementEdge struct{ to, reverse, capacity, cost int }
type placementGraph [][]placementEdge

func (g *placementGraph) vertex() int {
	id := len(*g)
	*g = append(*g, nil)
	return id
}

func (g placementGraph) connect(from, to, capacity, cost int) int {
	index := len(g[from])
	g[from] = append(g[from], placementEdge{to, len(g[to]), capacity, cost})
	g[to] = append(g[to], placementEdge{from, index, 0, -cost})
	return index
}

type replicaEdge struct {
	node       string
	from, edge int
}

func place(nodes []cpconfig.Node, previous []cpconfig.PartitionAssignment, policy Policy) ([][]string, string) {
	var graph placementGraph
	source, sink := graph.vertex(), graph.vertex()
	limits := make([]int, len(nodes))
	nodeVertices := make([]int, len(nodes))
	zones := make(map[string]struct{})
	eligible := 0
	for i, node := range nodes {
		nodeVertices[i] = graph.vertex()
		if policy.DistinctZones && node.Zone == "" {
			continue
		}
		eligible++
		zones[node.Zone] = struct{}{}
		limits[i] = len(previous)
		if limit, exists := policy.MaxReplicas[node.ID]; exists {
			limits[i] = min(limits[i], int(limit))
		}
		if node.CapacityBytes > 0 && policy.ReplicaBytes > 0 {
			limits[i] = int(min(uint64(limits[i]), node.CapacityBytes/policy.ReplicaBytes))
		}
		graph.connect(nodeVertices[i], sink, limits[i], 0)
	}
	edges := make([][]replicaEdge, len(previous))
	for partition, assignment := range previous {
		part := graph.vertex()
		graph.connect(source, part, int(policy.RF), 0)
		perZone := make(map[string]int)
		for i, node := range nodes {
			if limits[i] == 0 {
				continue
			}
			from := part
			if policy.DistinctZones {
				zone, ok := perZone[node.Zone]
				if !ok {
					zone = graph.vertex()
					graph.connect(part, zone, 1, 0)
					perZone[node.Zone] = zone
				}
				from = zone
			}
			cost := 1
			if slices.Contains(assignment.ReplicaNodeIDs, node.ID) {
				cost = 0
			}
			edge := graph.connect(from, nodeVertices[i], 1, cost)
			edges[partition] = append(edges[partition], replicaEdge{node.ID, from, edge})
		}
	}
	for graph.augment(source, sink) {
	}
	result := make([][]string, len(previous))
	for partition, group := range edges {
		for _, edge := range group {
			if graph[edge.from][edge.edge].capacity == 0 {
				result[partition] = append(result[partition], edge.node)
			}
		}
	}
	reason := "insufficient_capacity"
	if uint32(len(nodes)) < policy.RF {
		reason = "insufficient_nodes"
	} else if policy.DistinctZones &&
		(uint32(len(zones)) < policy.RF || uint32(eligible) < policy.RF) {
		reason = "insufficient_zones"
	}
	return result, reason
}

// augment finds a shortest residual path. Reverse edges allow a later
// partition to displace an earlier placement when capacity requires it.
func (g placementGraph) augment(source, sink int) bool {
	const unreachable = int(^uint(0) >> 1)
	distance, previous, via := make([]int, len(g)), make([]int, len(g)), make([]int, len(g))
	queued := make([]bool, len(g))
	for i := range distance {
		distance[i] = unreachable
	}
	distance[source] = 0
	queue := []int{source}
	queued[source] = true
	for len(queue) > 0 {
		from := queue[0]
		queue = queue[1:]
		queued[from] = false
		for index, edge := range g[from] {
			if edge.capacity == 0 || distance[edge.to] <= distance[from]+edge.cost {
				continue
			}
			distance[edge.to] = distance[from] + edge.cost
			previous[edge.to], via[edge.to] = from, index
			if !queued[edge.to] {
				queue = append(queue, edge.to)
				queued[edge.to] = true
			}
		}
	}
	if distance[sink] == unreachable {
		return false
	}
	for to := sink; to != source; {
		from := previous[to]
		edge := &g[from][via[to]]
		edge.capacity--
		g[to][edge.reverse].capacity++
		to = from
	}
	return true
}
