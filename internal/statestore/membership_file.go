package statestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// OpenMemory restores applied membership from a checkpoint owned by one CP
// process. On the first start only, groups may use bootstrap scrape addresses
// until Raft reports their membership. An empty path waits for Raft on startup.
func OpenMemory(path string) (*MemoryStore, error) {
	s := NewMemory()
	if path == "" {
		return s, nil
	}

	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := writeMemberships(path, s.memberships); err != nil {
			return nil, err
		}
		s.bootstrapDiscovery = true
	case err != nil:
		return nil, fmt.Errorf("read membership checkpoint: %w", err)
	default:
		var groups []cpconfig.GroupMembership
		if err := json.Unmarshal(data, &groups); err != nil {
			return nil, fmt.Errorf("decode membership checkpoint: %w", err)
		}
		if groups == nil {
			return nil, errors.New("membership checkpoint must contain a JSON array")
		}
		for _, m := range groups {
			key := groupKey{m.Region, m.PartitionID}
			if m.GroupID == "" || m.Index == 0 || len(m.Members) == 0 || s.memberships[key].Index != 0 {
				return nil, errors.New("invalid group in membership checkpoint")
			}
			s.memberships[key] = m
		}
	}
	s.membershipFile = path
	return s, nil
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
