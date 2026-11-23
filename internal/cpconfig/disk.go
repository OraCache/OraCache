package cpconfig

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const maxSavedSize = 16 << 20

// Disk scopes a client checkpoint to one deployment and process identity.
type Disk struct {
	Path    string
	Cluster string
	Region  string
	Client  string
	MaxAge  time.Duration
}

// Observation preserves the leader and conflict at the highest observed Raft term.
type Observation struct {
	Region    string `json:"region"`
	Partition uint32 `json:"partition"`
	Term      uint64 `json:"term"`
	Leader    string `json:"leader,omitempty"`
	Conflict  bool   `json:"conflict,omitempty"`
}

// Saved is the last accepted snapshot and the Gateway's term high-water marks.
type Saved struct {
	Config       *Config       `json:"config"`
	AcceptedAt   time.Time     `json:"accepted_at"`
	Observations []Observation `json:"raft_observations,omitempty"`
}

type diskPayload struct {
	Format  int    `json:"format"`
	Cluster string `json:"cluster"`
	Region  string `json:"region"`
	Client  string `json:"client"`
	Saved
}

type diskEnvelope struct {
	Payload  json.RawMessage `json:"payload"`
	Checksum [32]byte        `json:"sha256"`
}

func (d Disk) validate(saved Saved) error {
	if d.Cluster == "" || d.Region == "" || d.Client == "" || d.MaxAge <= 0 {
		return errors.New("checkpoint requires a cluster, region, client and positive max age")
	}
	if err := saved.Config.Validate(); err != nil {
		return err
	}
	if _, ok := saved.Config.RegionByID(d.Region); !ok {
		return errors.New("checkpoint has no local region")
	}
	type group struct {
		region    string
		partition uint32
	}
	seen := make(map[group]struct{}, len(saved.Observations))
	for _, obs := range saved.Observations {
		key := group{obs.Region, obs.Partition}
		_, duplicate := seen[key]
		if _, ok := saved.Config.Assignment(obs.Region, obs.Partition); !ok || obs.Term == 0 || duplicate {
			return errors.New("invalid checkpoint Raft observation")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (d Disk) Load(now time.Time) (Saved, error) {
	file, err := os.Open(d.Path)
	if err != nil {
		return Saved{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSavedSize+1))
	if err != nil {
		return Saved{}, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(data) > maxSavedSize {
		return Saved{}, errors.New("checkpoint is too large")
	}
	var envelope diskEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Saved{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	if sha256.Sum256(envelope.Payload) != envelope.Checksum {
		return Saved{}, errors.New("checkpoint checksum mismatch")
	}
	var payload diskPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return Saved{}, fmt.Errorf("decode checkpoint payload: %w", err)
	}
	if payload.Format != 1 || payload.Cluster != d.Cluster || payload.Region != d.Region || payload.Client != d.Client {
		return Saved{}, errors.New("checkpoint format or identity mismatch")
	}
	age := now.Sub(payload.AcceptedAt)
	if payload.AcceptedAt.IsZero() || age < 0 || age > d.MaxAge {
		return Saved{}, errors.New("checkpoint age outside allowed window")
	}
	if err := d.validate(payload.Saved); err != nil {
		return Saved{}, err
	}
	return payload.Saved, nil
}

func (d Disk) Save(saved Saved) error {
	if err := d.validate(saved); err != nil {
		return err
	}
	payload, err := json.Marshal(diskPayload{Format: 1, Cluster: d.Cluster, Region: d.Region, Client: d.Client, Saved: saved})
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	data, err := json.Marshal(diskEnvelope{Payload: payload, Checksum: sha256.Sum256(payload)})
	if err != nil {
		return fmt.Errorf("encode checkpoint envelope: %w", err)
	}
	if len(data) > maxSavedSize {
		return errors.New("checkpoint is too large")
	}

	dir := filepath.Dir(d.Path)
	tmp, err := os.CreateTemp(dir, ".oracache-checkpoint-*")
	if err != nil {
		return fmt.Errorf("create checkpoint: %w", err)
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = tmp.Sync()
	}
	if err := errors.Join(writeErr, syncErr, tmp.Close()); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := os.Rename(tmp.Name(), d.Path); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open checkpoint directory: %w", err)
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return nil
}
