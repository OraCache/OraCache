package cpconfig_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

func TestDiskSnapshotScopeAgeAndIntegrity(t *testing.T) {
	now := time.Now()
	disk := cpconfig.Disk{Path: filepath.Join(t.TempDir(), "config.json"), Cluster: "test", Region: "eu", Client: "gateway/a", MaxAge: time.Hour}
	config := cpconfigtest.Config(41)
	if err := disk.Save(cpconfig.Saved{Config: config, AcceptedAt: now}); err != nil {
		t.Fatal(err)
	}
	saved, err := disk.Load(now)
	if err != nil || saved.Config.Revision != 41 {
		t.Fatalf("load = %+v, %v", saved, err)
	}
	for _, wrong := range []cpconfig.Disk{
		{Path: disk.Path, Cluster: "other", Region: "eu", Client: disk.Client, MaxAge: time.Hour},
		{Path: disk.Path, Cluster: disk.Cluster, Region: "us", Client: disk.Client, MaxAge: time.Hour},
		{Path: disk.Path, Cluster: disk.Cluster, Region: disk.Region, Client: "agent/a", MaxAge: time.Hour},
	} {
		if _, err := wrong.Load(now); err == nil {
			t.Fatal("foreign snapshot accepted")
		}
	}
	if _, err := disk.Load(now.Add(2 * time.Hour)); err == nil {
		t.Fatal("expired snapshot accepted")
	}
	bytes, err := os.ReadFile(disk.Path)
	if err != nil {
		t.Fatal(err)
	}
	bytes[len(bytes)-4] ^= 1
	if err := os.WriteFile(disk.Path, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Load(now); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}
