package cpconfig_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

func TestDefaultPartitionCount(t *testing.T) {
	t.Parallel()
	if cpconfig.DefaultPartitionCount != 128 {
		t.Fatalf("default partition count = %d, want 128", cpconfig.DefaultPartitionCount)
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	t.Parallel()
	if err := cpconfigtest.Config(1).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidateRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*cpconfig.Config)
	}{
		{name: "zero_revision", mutate: func(c *cpconfig.Config) { c.Revision = 0 }},
		{name: "zero_partition_count", mutate: func(c *cpconfig.Config) { c.PartitionCount = 0 }},
		{name: "partition_count_above_max", mutate: func(c *cpconfig.Config) {
			c.PartitionCount = cpconfig.DefaultPartitionCount + 1
		}},
		{name: "no_regions", mutate: func(c *cpconfig.Config) { c.Regions = nil }},
		{name: "duplicate_region", mutate: func(c *cpconfig.Config) {
			c.Regions = append(c.Regions, c.Regions[0])
		}},
		{name: "no_master", mutate: func(c *cpconfig.Config) {
			c.Regions[0].Role = cpconfig.RegionRoleSlave
		}},
		{name: "two_masters", mutate: func(c *cpconfig.Config) {
			c.Regions[1].Role = cpconfig.RegionRoleMaster
		}},
		{name: "unknown_region_role", mutate: func(c *cpconfig.Config) {
			c.Regions[0].Role = cpconfig.RegionRoleUnspecified
		}},
		{name: "node_unknown_region", mutate: func(c *cpconfig.Config) {
			c.Nodes[0].Region = "moon"
		}},
		{name: "duplicate_node", mutate: func(c *cpconfig.Config) {
			c.Nodes = append(c.Nodes, c.Nodes[0])
		}},
		{name: "empty_node_address", mutate: func(c *cpconfig.Config) {
			c.Nodes[0].Address = ""
		}},
		{name: "unknown_node_state", mutate: func(c *cpconfig.Config) {
			c.Nodes[0].State = "exploded"
		}},
		{name: "partition_out_of_range", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].PartitionID = c.PartitionCount
		}},
		{name: "assignment_unknown_region", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].Region = "moon"
		}},
		{name: "duplicate_assignment", mutate: func(c *cpconfig.Config) {
			c.Partitions = append(c.Partitions, c.Partitions[0])
		}},
		{name: "no_replicas", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].ReplicaNodeIDs = nil
		}},
		{name: "no_leader", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].LeaderNodeID = ""
		}},
		{name: "leader_not_replica", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].LeaderNodeID = "us-1"
		}},
		{name: "unknown_replica", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].ReplicaNodeIDs[1] = "moon-1"
		}},
		{name: "replica_wrong_region", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].ReplicaNodeIDs[1] = "us-1"
		}},
		{name: "duplicate_replica", mutate: func(c *cpconfig.Config) {
			c.Partitions[0].ReplicaNodeIDs[1] = c.Partitions[0].ReplicaNodeIDs[0]
		}},
		{name: "missing_assignment", mutate: func(c *cpconfig.Config) {
			c.Partitions = c.Partitions[2:]
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := cpconfigtest.Config(1).Clone()
			tt.mutate(cfg)
			if err := cfg.Validate(); !errors.Is(err, cpconfig.ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestValidateRejectsNilConfig(t *testing.T) {
	t.Parallel()
	var cfg *cpconfig.Config
	if err := cfg.Validate(); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("nil Validate() error = %v, want ErrInvalidConfig", err)
	}
}

func TestLookups(t *testing.T) {
	t.Parallel()
	cfg := cpconfigtest.Config(7)

	if got := cfg.MasterRegion(); got != "eu" {
		t.Fatalf("MasterRegion() = %q, want eu", got)
	}
	if region, ok := cfg.RegionByID("us"); !ok || region.Role != cpconfig.RegionRoleSlave {
		t.Fatalf("RegionByID(us) = %+v, %v", region, ok)
	}
	if _, ok := cfg.RegionByID("moon"); ok {
		t.Fatal("RegionByID(moon) unexpectedly found")
	}
	if node, ok := cfg.NodeByID("us-2"); !ok || node.Address != "10.0.1.2:7000" {
		t.Fatalf("NodeByID(us-2) = %+v, %v", node, ok)
	}
	if _, ok := cfg.NodeByID("moon-1"); ok {
		t.Fatal("NodeByID(moon-1) unexpectedly found")
	}
	if a, ok := cfg.Assignment("eu", 1); !ok || a.LeaderNodeID != "eu-2" {
		t.Fatalf("Assignment(eu,1) = %+v, %v", a, ok)
	}
	if _, ok := cfg.Assignment("eu", 99); ok {
		t.Fatal("Assignment(eu,99) unexpectedly found")
	}
}

func TestCloneIsDeepCopy(t *testing.T) {
	t.Parallel()
	cfg := cpconfigtest.Config(1)
	clone := cfg.Clone()

	clone.Regions[0].GatewayEndpoints[0] = "changed"
	clone.Nodes[0].Address = "changed"
	clone.Partitions[0].ReplicaNodeIDs[0] = "changed"

	if cfg.Regions[0].GatewayEndpoints[0] == "changed" {
		t.Fatal("region gateway endpoints alias the original")
	}
	if cfg.Nodes[0].Address == "changed" {
		t.Fatal("nodes alias the original")
	}
	if cfg.Partitions[0].ReplicaNodeIDs[0] == "changed" {
		t.Fatal("replica ids alias the original")
	}
}

func TestProtoRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := cpconfigtest.Config(3)
	pb := cfg.ToProto()
	roundTripped, err := cpconfig.FromProto(pb)
	if err != nil {
		t.Fatalf("FromProto: %v", err)
	}
	if !proto.Equal(pb, roundTripped.ToProto()) {
		t.Fatalf("round trip mismatch:\nwant %v\ngot  %v", pb, roundTripped.ToProto())
	}

	// Mutating the protobuf must not mutate the domain snapshot, and vice versa.
	pb.Regions[0].GatewayEndpoints[0] = "changed"
	pb.Nodes[0].Address = "changed"
	pb.Partitions[0].ReplicaNodeIds[0] = "changed"
	if cfg.Regions[0].GatewayEndpoints[0] == "changed" ||
		cfg.Nodes[0].Address == "changed" ||
		cfg.Partitions[0].ReplicaNodeIDs[0] == "changed" {
		t.Fatal("domain config aliases protobuf slices")
	}
}

func TestFromProtoRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	if _, err := cpconfig.FromProto(nil); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("nil error = %v, want ErrInvalidConfig", err)
	}

	cfg := cpconfigtest.Config(1)
	pb := cfg.ToProto()
	pb.Regions[0].Role = controlplanev1.RegionRole_REGION_ROLE_UNSPECIFIED
	if _, err := cpconfig.FromProto(pb); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("unspecified role error = %v, want ErrInvalidConfig", err)
	}

	pb = cfg.ToProto()
	pb.Partitions[0].LeaderNodeId = "moon-1"
	if _, err := cpconfig.FromProto(pb); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("invalid leader error = %v, want ErrInvalidConfig", err)
	}
}

func TestRegionRoleText(t *testing.T) {
	t.Parallel()
	for _, role := range []cpconfig.RegionRole{cpconfig.RegionRoleMaster, cpconfig.RegionRoleSlave} {
		text, err := role.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%v): %v", role, err)
		}
		var parsed cpconfig.RegionRole
		if err := parsed.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if parsed != role {
			t.Fatalf("round trip = %v, want %v", parsed, role)
		}
	}
	var bogus cpconfig.RegionRole
	if err := bogus.UnmarshalText([]byte("leader")); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("unknown role error = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := cpconfigtest.Config(2)
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	validPath := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(validPath, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := cpconfig.LoadFile(validPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if loaded.Revision != cfg.Revision || loaded.MasterRegion() != "eu" {
		t.Fatalf("loaded = %+v", loaded)
	}

	unknownPath := filepath.Join(dir, "unknown.json")
	if err := os.WriteFile(unknownPath, []byte(`{"revision":1,"partition_count":1,"regions":[],"nodes":[],"partitions":[],"typo":true}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := cpconfig.LoadFile(unknownPath); err == nil {
		t.Fatal("LoadFile accepted unknown field")
	}

	badRolePath := filepath.Join(dir, "bad-role.json")
	badRole := append([]byte(nil), data...)
	var v any
	if err := json.Unmarshal(badRole, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	root := v.(map[string]any)
	root["regions"].([]any)[0].(map[string]any)["role"] = "leader"
	badRole, err = json.Marshal(root)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(badRolePath, badRole, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := cpconfig.LoadFile(badRolePath); err == nil {
		t.Fatal("LoadFile accepted unknown region role")
	}

	trailingPath := filepath.Join(dir, "trailing.json")
	if err := os.WriteFile(trailingPath, append(append([]byte(nil), data...), []byte("\n{}\n")...), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := cpconfig.LoadFile(trailingPath); err == nil {
		t.Fatal("LoadFile accepted trailing JSON value")
	}

	if _, err := cpconfig.LoadFile(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("LoadFile accepted missing file")
	}
}
