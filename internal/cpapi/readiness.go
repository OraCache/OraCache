package cpapi

import "context"

func WithMinimumRevision(revision uint64) Option {
	return func(s *Server) { s.minimumRevision = revision }
}

// Ready uses a current, linearizable StateStore read.
func (s *Server) Ready(ctx context.Context) bool {
	snapshot, err := s.store.Config(ctx)
	if err != nil {
		s.storeError(ctx, "readiness", err)
		return false
	}
	if snapshot.Config == nil || snapshot.Revision < s.minimumRevision {
		return false
	}
	return true
}

func (s *Server) storeError(ctx context.Context, operation string, err error) {
	s.metrics.IncCounter("oracache_cp_statestore_errors_total", map[string]string{"operation": operation})
	s.log.ErrorContext(ctx, "control plane store operation failed", "operation", operation, "error", err)
}
