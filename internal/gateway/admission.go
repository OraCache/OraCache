package gateway

// RouterOption sets a routing policy during construction.
type RouterOption func(*Router)

// WithMasterRegionReads restricts client reads until a follower region can
// prove its replication freshness. The policy applies to disk and live views.
func WithMasterRegionReads() RouterOption {
	return func(r *Router) { r.masterRegionReads = true }
}

func (r *Router) restrictReads(next *snapshot) {
	if r.masterRegionReads && r.region != next.masterRegion {
		for id, route := range next.local {
			next.local[id] = route.forOperation(true)
		}
	}
}
