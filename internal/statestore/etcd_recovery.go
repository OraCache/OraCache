package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// RecoverConfig republishes a restored snapshot while API and Controller
// processes are stopped. revision must exceed the externally recorded
// application high-water mark as well as the revision in the restored data.
func (s *EtcdStore) RecoverConfig(ctx context.Context, revision uint64) error {
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	state, err := s.readState(ctx, false)
	if err != nil {
		return err
	}
	if state.snapshot.Config == nil || revision <= state.snapshot.Revision {
		return errors.New("recovery requires a snapshot and a higher application revision")
	}
	config := state.snapshot.Config
	config.Revision = revision
	value, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode recovered config: %w", err)
	}
	resp, err := s.client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(s.configKey()), "=", state.configMod)).Then(
		clientv3.OpPut(s.configKey(), string(value)),
		clientv3.OpDelete(s.heartbeatPrefix(), clientv3.WithPrefix()),
		clientv3.OpDelete(s.receiptsKey()),
		clientv3.OpDelete(s.prefix+"/controller/", clientv3.WithPrefix()),
	).Commit()
	if err != nil {
		return fmt.Errorf("commit recovered config: %w", err)
	}
	if !resp.Succeeded {
		return errors.New("snapshot changed during recovery")
	}
	return nil
}
