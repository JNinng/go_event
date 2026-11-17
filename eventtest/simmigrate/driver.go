package simmigrate

import (
	"context"
	"fmt"
	"sync"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest/inmem"
)

// driver 是模拟 backend 的 event.Driver：DML 复用 inmem，所有操作前按
// 迁移模式 ensure（Create→Migrate；Verify→Verify 失败即返回 *VerifyError；
// None→不建也不校验，baseline 缺失时 DML 报错，模拟真实后端）。
type driver struct {
	backend *Backend
	mode    event.MigrateMode
	inner   event.Driver

	mu        sync.Mutex
	ensured   bool
	ensureErr error
}

func newDriver(backend *Backend, mode event.MigrateMode) event.Driver {
	return &driver{
		backend: backend,
		mode:    mode,
		inner:   inmem.New(),
	}
}

// ensure 首次操作前按迁移模式确保持久结构就绪；失败即缓存，后续操作直接
// 返回同一错误（Driver 负缓存，不在 Driver 内重试）。
func (d *driver) ensure(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ensured {
		return nil
	}
	if d.ensureErr != nil {
		return d.ensureErr
	}
	var err error
	switch d.mode {
	case event.MigrateCreate:
		err = d.backend.Migrate(ctx)
	case event.MigrateVerify:
		var report event.VerifyReport
		report, err = d.backend.Verify(ctx)
		if err == nil && !report.OK() {
			err = &event.VerifyError{Report: report}
		}
	case event.MigrateNone:
	default:
		err = fmt.Errorf("simmigrate: unsupported migrate mode %d: %w",
			int(d.mode), event.ErrMigrateModeUnsupported)
	}
	if err != nil {
		d.ensureErr = err
		return err
	}
	d.ensured = true
	return nil
}

// ready 在 ensure 之后检查 None 模式的 baseline 存在性：真实后端在缺表时
// DML 会失败，这里保持同样行为。
func (d *driver) ready(ctx context.Context) error {
	if err := d.ensure(ctx); err != nil {
		return err
	}
	if d.mode == event.MigrateNone && !d.backend.catalog.baselineReady() {
		return fmt.Errorf("simmigrate: baseline missing in None mode")
	}
	return nil
}

func (d *driver) Append(ctx context.Context, e event.RawEnvelope) error {
	if err := d.ready(ctx); err != nil {
		return err
	}
	return d.inner.Append(ctx, e)
}

func (d *driver) Fetch(ctx context.Context, q event.FetchQuery) ([]event.RawEnvelope, error) {
	if err := d.ready(ctx); err != nil {
		return nil, err
	}
	return d.inner.Fetch(ctx, q)
}

func (d *driver) HeadPosition(ctx context.Context, typ string) (string, error) {
	if err := d.ready(ctx); err != nil {
		return "", err
	}
	return d.inner.HeadPosition(ctx, typ)
}

func (d *driver) Claim(ctx context.Context, q event.ClaimQuery) ([]event.Lease, error) {
	if err := d.ready(ctx); err != nil {
		return nil, err
	}
	return d.inner.Claim(ctx, q)
}

func (d *driver) Heartbeat(ctx context.Context, h event.HeartbeatRequest) error {
	if err := d.ready(ctx); err != nil {
		return err
	}
	return d.inner.Heartbeat(ctx, h)
}

func (d *driver) Complete(ctx context.Context, c event.CompleteRequest) error {
	if err := d.ready(ctx); err != nil {
		return err
	}
	return d.inner.Complete(ctx, c)
}

func (d *driver) Fail(ctx context.Context, f event.FailRequest) error {
	if err := d.ready(ctx); err != nil {
		return err
	}
	return d.inner.Fail(ctx, f)
}
