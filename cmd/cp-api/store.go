package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type storeOptions struct {
	backend    string
	endpoints  string
	prefix     string
	configPath string
	nodeTTL    time.Duration
	devCluster bool
}

func openStore(ctx context.Context, options storeOptions) (statestore.Store, uint64, func(), error) {
	var store statestore.Store
	var publish func(context.Context, *cpconfig.Config) error
	closeStore := func() {}

	switch options.backend {
	case "memory":
		memory := statestore.NewMemory()
		store, publish = memory, memory.ApplyConfig
	case "etcd":
		endpoints := strings.Split(options.endpoints, ",")
		for i, endpoint := range endpoints {
			endpoints[i] = strings.TrimSpace(endpoint)
			if endpoints[i] == "" {
				return nil, 0, nil, errors.New("-etcd-endpoints must contain non-empty endpoints")
			}
		}
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 3 * time.Second,
			// The CP API logs store failures with the RPC outcome.
			Logger: zap.NewNop(),
		})
		if err != nil {
			return nil, 0, nil, fmt.Errorf("open etcd client: %w", err)
		}
		etcd, err := statestore.NewEtcd(client, options.prefix, options.nodeTTL)
		if err != nil {
			_ = client.Close()
			return nil, 0, nil, err
		}
		store, publish = etcd, etcd.BootstrapConfig
		closeStore = func() { _ = client.Close() }
	default:
		return nil, 0, nil, fmt.Errorf("unknown -store %q (use memory or etcd)", options.backend)
	}

	revision, err := bootstrapStore(ctx, store, publish, options)
	if err != nil {
		closeStore()
		return nil, 0, nil, err
	}
	return store, revision, closeStore, nil
}

func bootstrapStore(ctx context.Context, store statestore.Store, publish func(context.Context, *cpconfig.Config) error, options storeOptions) (uint64, error) {
	snapshot, err := store.Config(ctx)
	if err != nil {
		return 0, fmt.Errorf("read startup snapshot: %w", err)
	}
	if snapshot.Config != nil {
		return snapshot.Revision, nil
	}

	var config *cpconfig.Config
	switch {
	case options.devCluster:
		config = cpconfig.SyntheticDevCluster()
	case options.configPath != "":
		config, err = cpconfig.LoadFile(options.configPath)
		if err != nil {
			return 0, fmt.Errorf("load bootstrap config: %w", err)
		}
	default:
		return 0, nil
	}

	if err := publish(ctx, config); err != nil {
		return 0, fmt.Errorf("publish bootstrap config: %w", err)
	}
	// Another CP instance may have initialized the store first.
	snapshot, err = store.Config(ctx)
	if err != nil {
		return 0, fmt.Errorf("read bootstrapped snapshot: %w", err)
	}
	return snapshot.Revision, nil
}
