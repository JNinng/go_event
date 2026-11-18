package postgrec_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 驱动名 "pgx"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/postgrec"
	"github.com/jninng/go_event/postgresschema"
)

// 集成测试需真实 Postgres：设置 TEST_POSTGRES_DSN（如
// postgres://user:pass@localhost:5432/postgres）启用；未设置时跳过。
func testDB(t *testing.T) (*sql.DB, event.Driver, string) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skip Postgres integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	// 每次运行独立 schema：契约测试的类型隔离 + 运行间互不残留。
	schema := fmt.Sprintf("go_event_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
	})
	drv := postgrec.NewDriver(db, postgrec.WithSchema(schema),
		postgrec.WithRetention(7*24*time.Hour))
	return db, drv, schema
}

// claimStateReader 以 SQL 读取 event_claim 行的落库事实（status、
// last_error、failed_at），适配 eventtest.ClaimStateReader：契约套件据
// 此断言 Fail 错误摘要与终态时刻的写入口径。NULL 映射为空串/零值。
type claimStateReader struct {
	db     *sql.DB
	schema string
}

func (r claimStateReader) ReadClaimState(t *testing.T, eventID, consumerType string) (eventtest.ClaimState, bool) {
	t.Helper()
	var st eventtest.ClaimState
	var lastErr *string
	var failedAt *time.Time
	err := r.db.QueryRow(fmt.Sprintf(`
SELECT status, last_error, failed_at FROM %s.event_claim
WHERE event_id = $1 AND consumer_type = $2`, r.schema),
		eventID, consumerType).Scan(&st.Status, &lastErr, &failedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return eventtest.ClaimState{}, false // 行不存在
	}
	if err != nil {
		// 查询故障（连接断开等）不得折叠为「行不存在」：暴露真实错误。
		t.Fatalf("read claim state (%s, %s): %v", eventID, consumerType, err)
	}
	if lastErr != nil {
		st.LastError = *lastErr
	}
	if failedAt != nil {
		st.FailedAt = *failedAt
	}
	return st, true
}

// suiteDriver 在 Driver 之上挂接落库事实读取，满足 eventtest.ClaimStateReader。
type suiteDriver struct {
	event.Driver
	states claimStateReader
}

func (s suiteDriver) ReadClaimState(t *testing.T, eventID, consumerType string) (eventtest.ClaimState, bool) {
	return s.states.ReadClaimState(t, eventID, consumerType)
}

// 幂等 DDL：可重复执行（design.md §8）。
func TestMigrateIdempotent(t *testing.T) {
	db, _, _ := testDB(t)
	ctx := context.Background()
	schema := "go_event_migrate_" + fmt.Sprint(time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 1: %v", err)
	}
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 2 (must be idempotent): %v", err)
	}
	// 第三次显式 Migrate 亦幂等。
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 3: %v", err)
	}
}

// 全套契约测试（语义即测试，design.md §9.1）。
func TestPostgrecContractSuite(t *testing.T) {
	db, drv, schema := testDB(t)
	eventtest.RunSuite(t, suiteDriver{
		Driver: drv,
		states: claimStateReader{db: db, schema: schema},
	})
}

// Inspector 查询面契约（design.md §6/§9.2；探测方式：postgrec Driver 即
// event.Inspector）。
func TestPostgrecInspectorSuite(t *testing.T) {
	_, drv, _ := testDB(t)
	eventtest.RunInspectorSuite(t, drv)
}

// 延迟事件契约（deliver_at 定时投递，ADR 0013 §1）：零值立即、到期边界、
// 水位组合边界与信封透传。
func TestPostgrecDelayedDeliverySuite(t *testing.T) {
	_, drv, _ := testDB(t)
	eventtest.RunDelayedDeliverySuite(t, drv)
}

// Admin 管理面重放契约（写路径由独立 Admin 承载：postgrec.NewAdmin 构造，
// 与 Driver 共享连接与命名空间选项，不挂 Driver 类型）。重放事实经
// ClaimStateReader 装配（SQL 适配 event_claim 行）断言；互斥与操作来源
// 记录由 suiteAdmin 装配（套件注入点），互斥锁等待缩短为 300ms。
func TestPostgrecAdminReplaySuite(t *testing.T) {
	db, drv, schema := testDB(t)
	admin := suiteAdmin{
		Admin:  postgrec.NewAdmin(db, postgrec.WithSchema(schema), postgrec.WithAdminLockWait(300*time.Millisecond)),
		db:     db,
		schema: schema,
	}
	eventtest.RunAdminReplaySuite(t, suiteDriver{
		Driver: drv,
		states: claimStateReader{db: db, schema: schema},
	}, admin)
}

// suiteAdmin 挂接套件注入点：互斥锁占用（与迁移执行同一把 advisory 键）
// 与观测旁路挂接（等价 Admin 实例）。
type suiteAdmin struct {
	event.Admin
	db     *sql.DB
	schema string
}

// HoldExclusionLock 以会话级 advisory 锁占用命名空间互斥锁（postgrec.Admin
// 写路径在事务内 try-lock 同一键）：锁占用期间 Admin 写与迁移都必须
// 有界等待。锁必须钉在专用连接上（database/sql 连接池会把连接复用回池，
// 会话锁随连接滞留会污染后续操作），释放幂等、解锁后归还连接。
func (s suiteAdmin) HoldExclusionLock(t *testing.T) (release func()) {
	t.Helper()
	ctx := context.Background()
	holder, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire holder connection: %v", err)
	}
	if _, err := holder.ExecContext(ctx,
		"SELECT pg_advisory_lock(hashtext($1))", postgresschema.LockKey(s.schema)); err != nil {
		_ = holder.Close()
		t.Fatalf("hold exclusion lock: %v", err)
	}
	var once bool
	return func() {
		if once {
			return
		}
		once = true
		// 解锁与归还在同一清理：先解锁再 Close，避免会话锁滞留连接池。
		_, _ = holder.ExecContext(context.Background(),
			"SELECT pg_advisory_unlock(hashtext($1))", postgresschema.LockKey(s.schema))
		_ = holder.Close()
	}
}

// WithReplayObserver 返回挂接观测旁路的等价 Admin（原实例不变更）。
func (s suiteAdmin) WithReplayObserver(o event.Observer) event.Admin {
	return postgrec.NewAdmin(s.db, postgrec.WithSchema(s.schema), postgrec.WithObserver(o))
}

// Store 组合对象三面同源契约（便利构造，非能力面）：NewStore 构造的三面
// 共享同一连接与命名空间——Driver 落库事实对 Inspector 可见、Admin 重放
// 对 Driver 生效。各面完整语义仍由上述套件约束，此处不重复。
func TestPostgrecStoreSuite(t *testing.T) {
	db, _, schema := testDB(t)
	st := postgrec.NewStore(db, postgrec.WithSchema(schema))
	eventtest.RunStoreSuite(t, st.Driver, st.Inspector, st.Admin)
}

// 保留窗口惰性清理契约：每用例独立 schema + 短保留窗口的全新 Driver
// （design.md §5.2）。
func TestPostgrecRetentionSuite(t *testing.T) {
	db, _, _ := testDB(t)
	var seq atomic.Int64
	eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
		schema := fmt.Sprintf("go_event_ret_%d_%d", time.Now().UnixNano(), seq.Add(1))
		t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
		return postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithRetention(2*time.Second))
	})
}

// 遗留 failed 行（v2 加列前 failed_at 落 NULL，加列无回填）：FailedEvents
// 不报错，该行 FailedAt 映射为零值、摘要可见（升级边界，docs/api.md
// §2.10）。inmem 无 NULL 语义，不模拟该场景。
func TestPostgrecLegacyFailedRow_FailedEvents_MapsNullFailedAtToZeroValue(t *testing.T) {
	db, drv, schema := testDB(t)
	ctx := context.Background()
	typ := t.Name()

	if err := drv.Append(ctx, event.RawEnvelope{
		ID: "legacy-1", Type: typ, Payload: []byte(`{}`), OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err := drv.Claim(ctx, event.ClaimQuery{
		Type: typ, ConsumerType: "legacy", Lease: time.Minute, Limit: 1,
		RetryLimit: 5, Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ls := res.Leases
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	if err := drv.Fail(ctx, event.FailRequest{
		EventID: ls[0].Envelope.ID, ConsumerType: ls[0].ConsumerType, LeaseToken: ls[0].LeaseToken,
		Error: "legacy boom", RetryLimit: 1,
		Backoff: event.LinearBackoff(time.Millisecond, time.Millisecond), Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// 模拟遗留行：v2 升级前已存在的 failed 行 failed_at 为 NULL。
	if _, err := db.Exec(fmt.Sprintf(
		`UPDATE %s.event_claim SET failed_at = NULL WHERE event_id = $1`, schema), "legacy-1"); err != nil {
		t.Fatalf("null out failed_at: %v", err)
	}

	insp, ok := drv.(event.Inspector) // postgrec Driver 即 Inspector（类型断言探测）
	if !ok {
		t.Fatalf("driver %T must implement event.Inspector", drv)
	}
	out, err := insp.FailedEvents(ctx, event.FailedQuery{Type: typ, ConsumerType: "legacy", Limit: 10})
	if err != nil {
		t.Fatalf("failed events on NULL failed_at must not error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("failed events len = %d, want 1", len(out))
	}
	if !out[0].FailedAt.IsZero() {
		t.Fatalf("failed_at = %v, want zero value (NULL legacy row)", out[0].FailedAt)
	}
	if out[0].LastError != "legacy boom" {
		t.Fatalf("last_error = %q, want %q", out[0].LastError, "legacy boom")
	}
}
