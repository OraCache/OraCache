package shardraft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// The cache checkpoint stays opaque. A length-prefixed metadata envelope avoids
// base64-encoding the cache a second time and retains peer URLs after compaction.
type snapshotMetadata struct {
	Version    int
	Index      uint64
	Peers      map[uint64]Peer
	Removed    map[uint64]bool
	Operations map[string]membershipRecord
}

func (n *Instance) makeSnapshot() ([]byte, error) {
	cache, err := n.cache.Checkpoint()
	if err != nil {
		return nil, err
	}
	meta, err := json.Marshal(snapshotMetadata{Version: 1, Index: n.configIndex, Peers: n.peers, Removed: n.removed, Operations: n.operations})
	if err != nil {
		return nil, err
	}
	if uint64(len(meta)) > uint64(^uint32(0)) || int64(len(meta))+int64(len(cache))+4 > n.cfg.MaxWireBytes-wireOverhead {
		return nil, fmt.Errorf("checkpoint exceeds transport limit")
	}
	data := make([]byte, 4, 4+len(meta)+len(cache))
	binary.BigEndian.PutUint32(data, uint32(len(meta)))
	data = append(data, meta...)
	return append(data, cache...), nil
}

func (n *Instance) restoreSnapshot(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("missing snapshot metadata")
	}
	size := uint64(binary.BigEndian.Uint32(data))
	if size > uint64(len(data)-4) {
		return fmt.Errorf("invalid snapshot metadata length")
	}
	var meta snapshotMetadata
	if err := json.Unmarshal(data[4:4+size], &meta); err != nil {
		return err
	}
	if meta.Version != 1 || meta.Peers == nil || meta.Removed == nil || meta.Operations == nil {
		return fmt.Errorf("invalid snapshot metadata")
	}
	if err := n.cache.Restore(data[4+size:]); err != nil {
		return err
	}
	// Committed registry replaces seed hints. Existing send queues may remain
	// idle, but cannot grant membership or accept a removed peer's messages.
	n.peers = make(map[uint64]Peer, len(meta.Peers))
	for _, peer := range meta.Peers {
		if err := n.installPeer(peer); err != nil {
			return err
		}
	}
	n.removed, n.operations, n.configIndex = meta.Removed, meta.Operations, meta.Index
	return nil
}
