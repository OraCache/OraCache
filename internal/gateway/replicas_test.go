package gateway_test

import (
	"errors"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
)

func TestRouterReplicasAreRegionScoped(t *testing.T) {
	router, err := gateway.NewRouter("us")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Replicas(0, false); !errors.Is(err, gateway.ErrNoConfig) {
		t.Fatalf("want ErrNoConfig, got %v", err)
	}
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatal(err)
	}
	reads, err := router.Replicas(1, false) // local region us; leader first
	if err != nil || len(reads) != 2 || reads[0].NodeID != "us-2" || reads[1].NodeID != "us-1" {
		t.Fatalf("read replicas: %+v, %v", reads, err)
	}
	writes, err := router.Replicas(1, true) // master region eu
	if err != nil || len(writes) != 2 || writes[0].Region != "eu" {
		t.Fatalf("write replicas: %+v, %v", writes, err)
	}
	writes[0].NodeID = "mutated"
	again, _ := router.Replicas(1, true)
	if again[0].NodeID == "mutated" {
		t.Fatal("Replicas must return a copy")
	}
	if _, err := router.Replicas(cpconfigtest.PartitionCount, true); !errors.Is(err, gateway.ErrPartitionOutOfRange) {
		t.Fatalf("want out of range, got %v", err)
	}
}
