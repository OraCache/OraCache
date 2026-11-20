package main

import (
	"path/filepath"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
)

func TestOpenMemoryStoreWithDevCluster(t *testing.T) {
	store, revision, closeStore, err := openStore(t.Context(), storeOptions{backend: "memory", devCluster: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore()
	snapshot, err := store.Config(t.Context())
	if err != nil || snapshot.Config == nil || revision != snapshot.Config.Revision ||
		snapshot.Config.PartitionCount != cpconfig.DefaultPartitionCount {
		t.Fatalf("memory bootstrap revision = %d, snapshot = %+v, error = %v", revision, snapshot, err)
	}
}

func TestBootstrapStoreUsesPublishedSnapshotWithoutReadingFile(t *testing.T) {
	store := statestore.NewMemory()
	config := cpconfig.SyntheticDevCluster()
	config.Revision = 42
	if err := store.ApplyConfig(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	options := storeOptions{configPath: filepath.Join(t.TempDir(), "unavailable.json")}
	revision, err := bootstrapStore(t.Context(), store, store.ApplyConfig, options)
	if err != nil || revision != config.Revision {
		t.Fatalf("bootstrap revision = %d, error = %v, want %d", revision, err, config.Revision)
	}
}
