package postgrec_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgrec"
	"github.com/jninng/go_event/postgresschema"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Admin 加固集成测试（TEST_POSTGRES_DSN 门控）：与迁移的互斥（真实锁
// 竞争）、观测面（otel 指标 + slog + 类型化 Observer）与错误可诊断性。

// provisionFailedRow 构造一条 failed 终态行（终态 Fail 携带错误摘要）。
func provisionFailedRow(t *testing.T, drv event.Driver, id, typ, consumerType string) event.Lease {
	t.Helper()
	ctx := context.Background()
	if err := drv.Append(ctx, event.RawEnvelope{
		ID: id, Type: typ, Payload: []byte(`{}`), OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	ls, err := drv.Claim(ctx, event.ClaimQuery{
		Type: typ, ConsumerType: consumerType, Lease: time.Minute, Limit: 10,
		RetryLimit: 1, Now: time.Now(),
	})
	if err != nil || len(ls) != 1 {
		t.Fatalf("claim: %v, leases %d", err, len(ls))
	}
	if err := drv.Fail(ctx, event.FailRequest{
		EventID: ls[0].Envelope.ID, ConsumerType: ls[0].ConsumerType,
		LeaseToken: ls[0].LeaseToken, Error: "boom: order rejected",
		RetryLimit: 1, Backoff: event.LinearBackoff(time.Second, time.Second), Now: time.Now(),
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	return ls[0]
}

// holdMigrationKey 在专用连接上以会话级 advisory 锁占用命名空间互斥锁
// （与迁移执行、Admin 写路径同一键），返回释放函数。锁必须钉在专用连接
// 上（database/sql 连接池会把连接复用回池，会话锁滞留会污染后续操作）。
func holdMigrationKey(t *testing.T, db *sql.DB, schema string) (release func()) {
	t.Helper()
	ctx := context.Background()
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire holder connection: %v", err)
	}
	if _, err := holder.ExecContext(ctx,
		"SELECT pg_advisory_lock(hashtext($1))", postgresschema.LockKey(schema)); err != nil {
		_ = holder.Close()
		t.Fatalf("hold migration key: %v", err)
	}
	var once bool
	return func() {
		if once {
			return
		}
		once = true
		// 解锁与归还在同一清理：先解锁再 Close，避免会话锁滞留连接池。
		_, _ = holder.ExecContext(context.Background(),
			"SELECT pg_advisory_unlock(hashtext($1))", postgresschema.LockKey(schema))
		_ = holder.Close()
	}
}

// 真实竞争：持锁连接占用命名空间互斥键后，Admin 写与迁移各自有界等待并
// 返回各自的瞬时锁超时错误（同一把锁串行化双方）；释放后双方恢复成功。
func TestAdminReplayMutualExclusionWithMigration(t *testing.T) {
	db := migrationDB(t)
	restoreAdmin := postgrec.SetAdminLockWaitForTest(300 * time.Millisecond)
	defer restoreAdmin()
	restoreMigrate := postgrec.SetLockWaitTimeoutForTest(300 * time.Millisecond)
	defer restoreMigrate()

	ctx := context.Background()
	schema := newMigrationSchema(t, db)
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("provision schema: %v", err)
	}
	drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
	admin := postgrec.NewAdmin(db, postgrec.WithSchema(schema))
	// 预热 admin 的 ensure（首次操作按迁移模式处理持久结构）：互斥断言
	// 针对管理写路径的锁等待，而非 ensure 内迁移的锁等待。
	if ok, err := admin.ReplayFailed(ctx, "mx-1", "mx", time.Now()); ok || err != nil {
		t.Fatalf("warm-up replay = (%v, %v), want (false, nil)", ok, err)
	}
	provisionFailedRow(t, drv, "mx-1", "t.mutex", "mx")

	release := holdMigrationKey(t, db, schema)
	t.Cleanup(release)

	t.Run("HeldKey_AdminReplay_LockTimeout", func(t *testing.T) {
		start := time.Now()
		ok, err := admin.ReplayFailed(ctx, "mx-1", "mx", time.Now())
		if ok {
			t.Fatal("replay while migration lock held must not take effect, got ok=true")
		}
		if !errors.Is(err, event.ErrAdminLockTimeout) {
			t.Fatalf("replay while lock held = %v, want transient ErrAdminLockTimeout", err)
		}
		if elapsed := time.Since(start); elapsed >= 2*time.Second {
			t.Fatalf("lock wait = %v, want bounded under 2s", elapsed)
		}
	})
	t.Run("HeldKey_Migrate_LockTimeout", func(t *testing.T) {
		// 同一 schema 同一键：迁移也必须被持锁方阻塞（互斥是双向的，
		// 双方竞争同一把锁）。
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		if !errors.Is(err, event.ErrMigrationLockTimeout) {
			t.Fatalf("migrate while lock held = %v, want ErrMigrationLockTimeout", err)
		}
	})

	release()

	t.Run("ReleasedKey_AdminReplay_ReplayTakesEffect", func(t *testing.T) {
		ok, err := admin.ReplayFailed(ctx, "mx-1", "mx", time.Now())
		if err != nil {
			t.Fatalf("replay after release: %v", err)
		}
		if !ok {
			t.Fatal("failed row must be replayable after lock release, got ok=false")
		}
	})
	t.Run("ReleasedKey_Migrate_Succeeds", func(t *testing.T) {
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate after release: %v", err)
		}
	})
}

// 并发正向：迁移与 Admin 写同时在同一 schema 上发起，经同一把锁串行化，
// 双方都成功（互斥不是拒绝，等待有界）。
func TestAdminReplayAndMigrate_ConcurrentBothSucceed(t *testing.T) {
	db := migrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema := newMigrationSchema(t, db)
	admin := postgrec.NewAdmin(db, postgrec.WithSchema(schema))
	drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
	if err := drv.Append(ctx, event.RawEnvelope{
		ID: "cc-1", Type: "t.concurrent", Payload: []byte(`{}`), OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs[0] = postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
	}()
	go func() {
		defer wg.Done()
		<-start
		_, errs[1] = admin.ReplayFailed(ctx, "cc-1", "cc", time.Now())
	}()
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent op %d = %v, want nil (lock serializes both)", i, err)
		}
	}
}

// 执行故障不得折叠为 ok=false，且错误可判别：
//   - None 模式 + 表缺失：DML 直面 42P01，底层 pgconn 错误类型化透出
//     （error.type / SQLSTATE 判别）；
//   - Verify 模式 + 表缺失：迁移面哨兵 *VerifyError 经 ensure 原样上抛。
func TestAdminReplay_MissingTables_ReturnsErrorNotNoEffect(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("NoneMode_MissingTables_DMLFaultIsTypedError", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		admin := postgrec.NewAdmin(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateNone))

		ok, err := admin.ReplayFailed(ctx, "none-1", "none", time.Now())
		if ok {
			t.Fatal("replay without tables must not report ok=true")
		}
		if err == nil {
			t.Fatal("replay without tables must return an error (execution faults must not fold into ok=false)")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("error = %v, want typed pgconn error with SQLSTATE 42P01", err)
		}
	})
	t.Run("VerifyMode_MissingTables_ReturnsVerifyError", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		admin := postgrec.NewAdmin(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateVerify))

		ok, err := admin.ReplayFailed(ctx, "verify-1", "verify", time.Now())
		if ok {
			t.Fatal("replay without tables must not report ok=true")
		}
		if err == nil {
			t.Fatal("replay without tables must return an error")
		}
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("error = %v, want typed *event.VerifyError (ensure surface)", err)
		}
	})
}

// adminObserverCollector 收集管理面重放观测事件。
type adminObserverCollector struct {
	mu     sync.Mutex
	events []event.AdminReplayEvent
}

func (c *adminObserverCollector) OnPublish(event.PublishEvent)     {}
func (c *adminObserverCollector) OnConsume(event.ConsumeEvent)     {}
func (c *adminObserverCollector) OnWake(event.WakeEvent)           {}
func (c *adminObserverCollector) OnLeaseAbandoned(event.LeaseEvent) {}
func (c *adminObserverCollector) OnRunnerStopped(event.StopEvent)  {}

func (c *adminObserverCollector) OnAdminReplay(e event.AdminReplayEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *adminObserverCollector) collected() []event.AdminReplayEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]event.AdminReplayEvent(nil), c.events...)
}

// recordHandler 捕获完整 slog 记录（含属性）供断言。
type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordHandler) find(msg, key string) (slog.Value, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		var found slog.Value
		var ok bool
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key && !ok {
				found, ok = a.Value, true
				return false
			}
			return true
		})
		if ok {
			return found, true
		}
	}
	return slog.Value{}, false
}

// adminReplayDatapoint 收集 result 属性匹配的 <scope>.admin.replay 数据点
// 值总和（指标未出现返回 0）。
func adminReplayDatapoint(t *testing.T, reader *sdkmetric.ManualReader, scope, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != scope+".admin.replay" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s.admin.replay type %T, want Sum[int64]", scope, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value("result"); ok && v.AsString() == result {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// 观测面集成（操作来源记录）：重放结果逐次发射指标 / 日志 / Observer
// 事件，结果枚举与返回值一致；日志含操作与事件 × 身份。
func TestAdminReplayObservability(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()
	schema := newMigrationSchema(t, db)
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("provision schema: %v", err)
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	logs := &recordHandler{}
	obs := &adminObserverCollector{}

	admin := postgrec.NewAdmin(db, postgrec.WithSchema(schema),
		postgrec.WithMeterProvider(mp), postgrec.WithLogger(slog.New(logs)),
		postgrec.WithObserver(obs))
	drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
	provisionFailedRow(t, drv, "ob-1", "t.obs", "ob")

	ok, err := admin.ReplayFailed(ctx, "ob-1", "ob", time.Now())
	if err != nil || !ok {
		t.Fatalf("replay = (%v, %v), want (true, nil)", ok, err)
	}
	t.Run("ReplayedResult_CountsOKAndLogsAndObserves", func(t *testing.T) {
		if got := adminReplayDatapoint(t, reader, "postgrec", "ok"); got != 1 {
			t.Fatalf("admin.replay ok count = %d, want 1", got)
		}
		v, found := logs.find("postgrec: admin replay", "event_id")
		if !found || v.String() != "ob-1" {
			t.Fatalf("log event_id = (%q, %v), want ob-1", v.String(), found)
		}
		if v, found := logs.find("postgrec: admin replay", "consumer_type"); !found || v.String() != "ob" {
			t.Fatalf("log consumer_type = (%q, %v), want ob", v.String(), found)
		}
		if v, found := logs.find("postgrec: admin replay", "result"); !found || v.String() != event.AdminReplayOK {
			t.Fatalf("log result = (%q, %v), want ok", v.String(), found)
		}
		events := obs.collected()
		if len(events) != 1 || events[0].Result != event.AdminReplayOK ||
			events[0].EventID != "ob-1" || events[0].ConsumerType != "ob" || events[0].Err != nil {
			t.Fatalf("observer events = %+v, want one ok event for (ob-1, ob)", events)
		}
	})

	t.Run("RepeatedReplay_CountsNoEffect", func(t *testing.T) {
		// 行已重放为 retrying：再次重放无效果，结果枚举与返回值一致。
		ok, err := admin.ReplayFailed(ctx, "ob-1", "ob", time.Now())
		if err != nil || ok {
			t.Fatalf("repeat replay = (%v, %v), want (false, nil)", ok, err)
		}
		if got := adminReplayDatapoint(t, reader, "postgrec", "no_effect"); got != 1 {
			t.Fatalf("admin.replay no_effect count = %d, want 1", got)
		}
		events := obs.collected()
		if len(events) != 2 || events[1].Result != event.AdminReplayNoEffect {
			t.Fatalf("observer events = %+v, want second no_effect event", events)
		}
	})

	t.Run("LockTimeout_CountsLockTimeoutAndObservesError", func(t *testing.T) {
		defer postgrec.SetAdminLockWaitForTest(200 * time.Millisecond)()
		obs2 := &adminObserverCollector{}
		locked := postgrec.NewAdmin(db, postgrec.WithSchema(schema),
			postgrec.WithMeterProvider(mp), postgrec.WithObserver(obs2))
		// 预热 ensure（锁占用期间 ensure 的迁移会先撞锁）：预热后互斥
		// 断言落在管理写路径上。ob-1 已重放为 retrying，无效果。
		if ok, err := locked.ReplayFailed(ctx, "ob-1", "ob", time.Now()); ok || err != nil {
			t.Fatalf("warm-up replay = (%v, %v), want (false, nil)", ok, err)
		}
		release := holdMigrationKey(t, db, schema)
		t.Cleanup(release)

		ok, err := locked.ReplayFailed(ctx, "ob-1", "ob", time.Now())
		if ok || !errors.Is(err, event.ErrAdminLockTimeout) {
			t.Fatalf("replay while lock held = (%v, %v), want ErrAdminLockTimeout", ok, err)
		}
		if got := adminReplayDatapoint(t, reader, "postgrec", "lock_timeout"); got != 1 {
			t.Fatalf("admin.replay lock_timeout count = %d, want 1", got)
		}
		// 预热重放（无效果）与锁超时重放各发一次事件：取 lock_timeout
		// 事件断言（恰一次）。
		var timedOut []event.AdminReplayEvent
		for _, e := range obs2.collected() {
			if e.Result == event.AdminReplayLockTimeout {
				timedOut = append(timedOut, e)
			}
		}
		if len(timedOut) != 1 {
			t.Fatalf("lock_timeout observer events = %d, want 1", len(timedOut))
		}
		if !errors.Is(timedOut[0].Err, event.ErrAdminLockTimeout) {
			t.Fatalf("observer err = %v, want ErrAdminLockTimeout", timedOut[0].Err)
		}
	})

	t.Run("CanceledContext_CountsCanceledAndSkipsErrorLog", func(t *testing.T) {
		// 调用方取消：结果枚举 canceled（异常路径），canceled 不记错误日志。
		ctxCancel, cancel := context.WithCancel(ctx)
		cancel()
		defer cancel()
		ok, err := admin.ReplayFailed(ctxCancel, "ob-1", "ob", time.Now())
		if ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled replay = (%v, %v), want (false, context.Canceled)", ok, err)
		}
		if got := adminReplayDatapoint(t, reader, "postgrec", "canceled"); got != 1 {
			t.Fatalf("admin.replay canceled count = %d, want 1", got)
		}
		// 共享观察者此前已收 ok + no_effect 两事件，本次为第三个。
		events := obs.collected()
		if len(events) != 3 || events[2].Result != event.AdminReplayCanceled {
			t.Fatalf("observer events = %+v, want third canceled event", events)
		}
		if !errors.Is(events[2].Err, context.Canceled) {
			t.Fatalf("observer err = %v, want context.Canceled", events[2].Err)
		}
		if _, found := logs.find("postgrec: admin replay failed", "op"); found {
			t.Fatal("canceled replay must not be logged as error (admin replay failed)")
		}
		if v, found := logs.find("postgrec: admin replay canceled", "result"); !found || v.String() != event.AdminReplayCanceled {
			t.Fatalf("canceled log result = (%q, %v), want %s", v.String(), found, event.AdminReplayCanceled)
		}
	})

	t.Run("ExecutionFault_CountsError", func(t *testing.T) {
		// None 模式 + 缺表：ensure 失败为执行故障（非无效果）。
		fresh := newMigrationSchema(t, db)
		faulted := postgrec.NewAdmin(db, postgrec.WithSchema(fresh),
			postgrec.WithMigrateMode(event.MigrateNone), postgrec.WithMeterProvider(mp),
			postgrec.WithObserver(obs))
		ok, err := faulted.ReplayFailed(ctx, "ob-1", "ob", time.Now())
		if ok || err == nil {
			t.Fatalf("replay without tables = (%v, %v), want (false, error)", ok, err)
		}
		if got := adminReplayDatapoint(t, reader, "postgrec", "error"); got != 1 {
			t.Fatalf("admin.replay error count = %d, want 1", got)
		}
	})
}
