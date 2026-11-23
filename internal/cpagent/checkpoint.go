package cpagent

import (
	"context"
	"errors"

	"github.com/OraCache/oracache/internal/cpconfig"
)

func (a *Agent) restore() error {
	if a.disk == nil || a.Config() != nil {
		return nil
	}
	saved, err := a.disk.Load(a.clock.Now())
	if err != nil {
		return err
	}
	node, ok := saved.Config.NodeByID(a.nodeID)
	if !ok || node.Region != a.region {
		return errors.New("checkpoint has no local storage node")
	}
	a.cold.Store(true)
	if err := a.holder.Apply(saved.Config); err != nil {
		a.cold.Store(false)
		return err
	}
	return nil
}

func (a *Agent) save(ctx context.Context) {
	if a.disk == nil {
		return
	}
	if err := a.disk.Save(cpconfig.Saved{Config: a.Config(), AcceptedAt: a.clock.Now()}); err != nil {
		a.log.WarnContext(ctx, "save agent checkpoint failed", "error", err)
	}
}
