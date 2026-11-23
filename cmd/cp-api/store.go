package main

import (
	"context"
	"fmt"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cplink"
	"github.com/OraCache/oracache/internal/statestore"
)

type storeOptions struct {
	backend         string
	endpoints       string
	prefix          string
	configPath      string
	nodeTTL         time.Duration
	devCluster      bool
	minimumRevision uint64
}

func openStore(ctx context.Context, options storeOptions) (statestore.Store, uint64, func(), error) {
	return configureStore(ctx, options, true)
}

func startStore(ctx context.Context, options storeOptions) (statestore.Store, uint64, func(), error) {
	return configureStore(ctx, options, options.backend != "etcd" || options.configPath != "" || options.devCluster)
}

func configureStore(ctx context.Context, options storeOptions, bootstrap bool) (statestore.Store, uint64, func(), error) {
	var store statestore.Store
	var publish func(context.Context, *cpconfig.Config) error
	closeStore := func() {}

	switch options.backend {
	case "memory":
		memory := statestore.NewMemory()
		store, publish = memory, memory.ApplyConfig
	case "etcd":
		client, err := cplink.DialEtcd(options.endpoints)
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

	if !bootstrap {
		return store, 0, closeStore, nil
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

func recoverConfig(ctx context.Context, options storeOptions, revision uint64) error {
	if options.backend != "etcd" {
		return fmt.Errorf("recovery requires -store etcd")
	}
	client, err := cplink.DialEtcd(options.endpoints)
	if err != nil {
		return err
	}
	defer client.Close()
	store, err := statestore.NewEtcd(client, options.prefix, options.nodeTTL)
	if err != nil {
		return err
	}
	return store.RecoverConfig(ctx, revision)
}
