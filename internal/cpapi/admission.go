package cpapi

import "github.com/OraCache/oracache/internal/cpconfig"

// WithMasterReadsOnly keeps Gateway reads in the master until there is a
// replication freshness contract for follower regions.
func WithMasterReadsOnly(enabled bool) Option {
	return func(s *Server) { s.masterReadsOnly = enabled }
}

func restrictRegionalReads(config *cpconfig.Config) {
	master := config.MasterRegion()
	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		if assignment.Region != master {
			assignment.Readable = false
			assignment.ReadyReplicaNodeIDs = nil
		}
	}
}
