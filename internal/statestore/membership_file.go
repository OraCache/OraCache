package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// OpenMemory restores applied membership from a checkpoint owned by one CP
// process. On the first start only, groups may use bootstrap scrape addresses
// until Raft reports their membership. An empty path waits for Raft on startup.
// On a checkpoint error it returns a usable store awaiting fresh Raft reports.
func OpenMemory(path string) (*MemoryStore, error) {
	s := NewMemory()
	if path == "" {
		return s, nil
	}

	s.membershipFile = path
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := writeMemberships(path, s.memberships); err != nil {
			return s, err
		}
		s.bootstrapDiscovery = true
	case err != nil:
		return s, fmt.Errorf("read membership checkpoint: %w", err)
	default:
		var groups []cpconfig.GroupMembership
		if err := json.Unmarshal(data, &groups); err != nil {
			return s, fmt.Errorf("decode membership checkpoint: %w", err)
		}
		if groups == nil {
			return s, errors.New("membership checkpoint must contain a JSON array")
		}
		loaded := make(map[groupKey]cpconfig.GroupMembership, len(groups))
		for _, m := range groups {
			key := groupKey{m.Region, m.PartitionID}
			if m.GroupID == "" || m.Index == 0 || len(m.Members) == 0 || loaded[key].Index != 0 {
				return s, errors.New("invalid group in membership checkpoint")
			}
			loaded[key] = m
		}
		s.memberships = loaded
	}
	return s, nil
}

// DiscoveryConfig checkpoints the membership before publishing it to Prometheus.
// The separate lock serializes disk writes without delaying heartbeat ingestion
// or routing. A failed refresh lets Prometheus keep the last published targets.
func (s *MemoryStore) DiscoveryConfig(ctx context.Context, now time.Time, ttl time.Duration) (Snapshot, error) {
	if s.membershipFile == "" {
		return s.LiveConfig(ctx, now, ttl)
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if ttl <= 0 {
		return Snapshot{}, errors.New("statestore: node TTL must be positive")
	}
	if !s.checkpointMu.TryLock() {
		return Snapshot{}, errors.New("membership checkpoint refresh in progress")
	}
	defer s.checkpointMu.Unlock()

	s.mu.RLock()
	view := s.liveConfigLocked(now, ttl)
	version := s.membershipVersion
	if version == s.checkpointVersion {
		s.mu.RUnlock()
		return view, nil
	}
	memberships := maps.Clone(s.memberships)
	s.mu.RUnlock()

	if err := writeMemberships(s.membershipFile, memberships); err != nil {
		return Snapshot{}, err
	}
	s.checkpointVersion = version
	return view, nil
}

func writeMemberships(path string, groups map[groupKey]cpconfig.GroupMembership) error {
	memberships := make([]cpconfig.GroupMembership, 0, len(groups))
	memberships = slices.AppendSeq(memberships, maps.Values(groups))
	f, err := os.CreateTemp(filepath.Dir(path), ".membership-*")
	if err != nil {
		return fmt.Errorf("create membership checkpoint: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	err = json.NewEncoder(f).Encode(memberships)
	if err == nil {
		err = f.Sync()
	}
	if err := errors.Join(err, f.Close()); err != nil {
		return fmt.Errorf("write membership checkpoint: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("replace membership checkpoint: %w", err)
	}
	return nil
}
