package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/OraCache/oracache/internal/scheduler"
	"github.com/OraCache/oracache/internal/statestore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const reconcileInterval = 250 * time.Millisecond

type Controller struct {
	client *clientv3.Client
	store  *statestore.EtcdStore
	root   string
}

func New(client *clientv3.Client, namespace string, ttl time.Duration) (*Controller, error) {
	store, err := statestore.NewEtcd(client, namespace, ttl)
	if err != nil {
		return nil, err
	}
	return &Controller{client: client, store: store, root: strings.TrimRight(namespace, "/") + "/controller/"}, nil
}

func (c *Controller) pending() string   { return c.root + "pending/" }
func (c *Controller) results() string   { return c.root + "results/" }
func (c *Controller) elections() string { return c.root + "election" }

func (c *Controller) Run(ctx context.Context, id string, ttl int, log *slog.Logger) error {
	if id == "" || ttl < 2 {
		return errors.New("Controller requires an ID and election lease of at least two seconds")
	}
	for ctx.Err() == nil {
		if err := c.serve(ctx, id, ttl, log); err != nil && ctx.Err() == nil {
			log.WarnContext(ctx, "controller session ended", "error", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
	return nil
}

func (c *Controller) serve(ctx context.Context, id string, ttl int, log *slog.Logger) error {
	ctx = clientv3.WithRequireLeader(ctx)
	session, err := concurrency.NewSession(c.client, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("create Controller lease: %w", err)
	}
	defer session.Close()
	election := concurrency.NewElection(session, c.elections())
	campaign, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-session.Done():
			cancel()
		case <-campaign.Done():
		}
	}()
	defer func() { cancel(); <-stopped }()
	if err := election.Campaign(campaign, id); err != nil {
		return fmt.Errorf("campaign for Controller leadership: %w", err)
	}
	fence := statestore.ElectionFence{Key: election.Key(), Revision: election.Rev(), Lease: session.Lease()}
	log.InfoContext(ctx, "controller elected", "controller_id", id)
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for campaign.Err() == nil {
		if err := c.reconcile(campaign, fence); err != nil {
			return err
		}
		select {
		case <-campaign.Done():
		case <-ticker.C:
		}
	}
	return nil
}

func (c *Controller) reconcile(ctx context.Context, fence statestore.ElectionFence) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	pending, err := c.client.Get(ctx, c.pending(), clientv3.WithFirstCreate()...)
	if err != nil {
		return fmt.Errorf("read pending topology intents: %w", err)
	}
	if len(pending.Kvs) == 0 {
		return nil
	}
	kv := pending.Kvs[0]
	desired, revision, err := c.store.VersionedConfig(ctx)
	if err != nil {
		return err
	}
	var plan scheduler.Plan
	var result Result
	result.Revision = desired.Revision
	err = json.Unmarshal(kv.Value, &plan)
	if err != nil {
		result.Error = "invalid topology intent"
	} else if changed, err := Validate(desired.Config, plan); err != nil {
		result.Error = err.Error()
	} else if changed {
		result.Revision, result.Changed = plan.Proposed.Revision, true
	}
	var proposed = plan.Proposed
	if result.Changed {
		proposed = staticConfig(proposed)
		proposed.Revision = result.Revision
	} else {
		proposed = nil
	}
	value, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode topology result: %w", err)
	}
	id := strings.TrimPrefix(string(kv.Key), c.pending())
	return c.store.CommitIntent(ctx, proposed, revision, fence, string(kv.Key), kv.ModRevision, c.results()+id, string(value))
}
