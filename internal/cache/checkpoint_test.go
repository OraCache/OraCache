package cache

import (
	"context"
	"testing"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
)

func TestCheckpointRestoresVersionsAndDeletedCounters(t *testing.T) {
	ctx := context.Background()
	source := New()
	for _, key := range []string{"live", "deleted"} {
		for _, data := range []string{"old", "new"} {
			if _, err := source.Put(ctx, &pb.PutRequest{Key: key, Data: []byte(data)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	source.Delete(ctx, &pb.DeleteRequest{Key: "deleted"})
	data, err := source.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	// A captured checkpoint must remain immutable after more writes.
	source.Put(ctx, &pb.PutRequest{Key: "live", Data: []byte("later")})
	restored := New()
	restored.Put(ctx, &pb.PutRequest{Key: "unrelated"})
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	for rev, expected := range map[uint64]string{1: "old", 2: "new"} {
		g, err := restored.Get(ctx, &pb.GetRequest{Key: "live", Rev: rev})
		if err != nil || string(g.Data) != expected || g.LatestRev != 2 {
			t.Fatalf("restored version: %v %v", g, err)
		}
	}
	g, _ := restored.Get(ctx, &pb.GetRequest{Key: "unrelated"})
	if g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("restore merged instead of replacing")
	}
	g, _ = restored.Get(ctx, &pb.GetRequest{Key: "deleted"})
	if g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("deleted key restored as live")
	}
	p, err := restored.Put(ctx, &pb.PutRequest{Key: "deleted"})
	if err != nil || p.Rev != 3 {
		t.Fatalf("deleted sequence lost: %v %v", p, err)
	}
	if err := restored.Restore([]byte(`{"format":1,"keys":{"broken":{"sequence":1,"versions":{"2":"eA=="}}}}`)); err == nil {
		t.Fatal("invalid revision accepted")
	}
	g, _ = restored.Get(ctx, &pb.GetRequest{Key: "live"})
	if string(g.Data) != "new" {
		t.Fatal("failed restore corrupted current state")
	}
}
