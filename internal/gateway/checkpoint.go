package gateway

import (
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// Restore loads a previously accepted routing map for reads and term fencing.
func (p *Poller) Restore() error {
	if p.Disk == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	saved, err := p.Disk.Load(now)
	if err != nil {
		return err
	}
	return p.Router.applyView(saved.Config, &saved, now)
}

func checkpointObservations(next *snapshot, records []cpconfig.Observation) *snapshot {
	prior := &snapshot{
		revision: next.revision, partitionCount: next.partitionCount,
		local: make(map[uint32]partitionRoute), write: make(map[uint32]partitionRoute),
	}
	for _, record := range records {
		route := partitionRoute{region: record.Region, observation: leaderObservation{
			term: record.Term, nodeID: record.Leader, conflict: record.Conflict,
		}}
		if next.local[record.Partition].region == record.Region {
			prior.local[record.Partition] = route
		}
		if next.write[record.Partition].region == record.Region {
			prior.write[record.Partition] = route
		}
	}
	return prior
}

func (r *Router) checkpoint(config *cpconfig.Config) cpconfig.Saved {
	current := r.cur.Load()
	saved := cpconfig.Saved{Config: config, AcceptedAt: current.acceptedAt}
	for i, routes := range []map[uint32]partitionRoute{current.local, current.write} {
		for partition, route := range routes {
			if route.observation.term == 0 {
				continue
			}
			if i == 1 && current.local[partition].region == route.region {
				continue
			}
			saved.Observations = append(saved.Observations, cpconfig.Observation{
				Region: route.region, Partition: partition, Term: route.observation.term,
				Leader: route.observation.nodeID, Conflict: route.observation.conflict,
			})
		}
	}
	return saved
}

// AcceptedAt reports when the most recent full routing view was accepted.
func (r *Router) AcceptedAt() time.Time {
	if s := r.cur.Load(); s != nil {
		return s.acceptedAt
	}
	return time.Time{}
}

// Confirmed reports whether this process has received its routing map from CP.
func (r *Router) Confirmed() bool {
	s := r.cur.Load()
	return s != nil && !s.cold
}
