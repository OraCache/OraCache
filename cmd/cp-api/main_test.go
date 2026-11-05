package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/OraCache/oracache/internal/statestore"
)

func TestNewServerRegistersControlPlaneAPI(t *testing.T) {
	t.Parallel()
	server := newServer(statestore.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(server.Stop)
	services := server.GetServiceInfo()
	if _, ok := services["controlplane.v1.ControlPlaneAPI"]; !ok {
		t.Fatalf("ControlPlaneAPI is not registered: %v", services)
	}
}
