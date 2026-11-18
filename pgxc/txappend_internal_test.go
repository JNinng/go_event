package pgxc

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jninng/go_event"
)

// TransactionalAppender 入口语义单测（无 DB，注入替身）：ensure 失败上抛
// 且不触碰调用方事务；取消在 ensure 之前短路；ensure 通过后 INSERT 落在
// 调用方事务上（库不 commit、不 rollback）。执行路径与提交后可见性由
// TEST_POSTGRES_DSN 门控的契约套件与集成测试覆盖。

// recordingTx 是 pgx.Tx 的替身：仅记录 Exec；其余方法不触达（嵌入 nil
// 接口，误用即 panic——AppendTx 契约只允许 Exec）。
type recordingTx struct {
	pgx.Tx
	execCalls atomic.Int64
	execErr   error
	lastSQL   string
}

func (t *recordingTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	t.execCalls.Add(1)
	t.lastSQL = sql
	if t.execErr != nil {
		return pgconn.CommandTag{}, t.execErr
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// ensure 失败（迁移面错误）：原样上抛，调用方事务零触碰（事务控制归调用方）。
func TestTransactionalAppender_AppendTx_EnsureFailure_ReturnsErrorWithoutTouchingTx(t *testing.T) {
	boom := errors.New("boom migrate")
	d := testDriver(event.MigrateCreate, &fakeManager{migrateErr: boom}, newFakeClock())
	ta := newTransactionalAppenderOn(d)
	tx := &recordingTx{}

	err := ta.AppendTx(context.Background(), tx, event.RawEnvelope{
		ID: "ta-1", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
	})
	if !errors.Is(err, boom) {
		t.Fatalf("append tx = %v, want ensure error", err)
	}
	if tx.execCalls.Load() != 0 {
		t.Fatalf("exec calls = %d, want 0 (ensure failure must not touch the caller tx)", tx.execCalls.Load())
	}
}

// 调用方取消：在 ensure 之前短路，不触碰迁移面与调用方事务。
func TestTransactionalAppender_AppendTx_CanceledContext_ShortCircuitsBeforeEnsure(t *testing.T) {
	mgr := &fakeManager{}
	d := testDriver(event.MigrateCreate, mgr, newFakeClock())
	ta := newTransactionalAppenderOn(d)
	tx := &recordingTx{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer cancel()
	err := ta.AppendTx(ctx, tx, event.RawEnvelope{
		ID: "ta-2", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("append tx = %v, want context.Canceled", err)
	}
	if mgr.migrateCalls.Load() != 0 {
		t.Fatalf("migrate calls = %d, want 0 (canceled ctx must short-circuit)", mgr.migrateCalls.Load())
	}
	if tx.execCalls.Load() != 0 {
		t.Fatalf("exec calls = %d, want 0", tx.execCalls.Load())
	}
}

// ensure 通过：INSERT 落在调用方事务上（一次 Exec，共用语句，不 commit）。
func TestTransactionalAppender_AppendTx_EnsurePassed_ExecutesSharedInsertOnCallerTx(t *testing.T) {
	d := testDriver(event.MigrateNone, &fakeManager{}, newFakeClock())
	ta := newTransactionalAppenderOn(d)
	tx := &recordingTx{}

	err := ta.AppendTx(context.Background(), tx, event.RawEnvelope{
		ID: "ta-3", Type: "t", Payload: []byte(`{}`),
		Metadata:   map[string]string{"k": "v"},
		OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("append tx: %v", err)
	}
	if tx.execCalls.Load() != 1 {
		t.Fatalf("exec calls = %d, want 1", tx.execCalls.Load())
	}
	if !strings.Contains(tx.lastSQL, "INSERT INTO") ||
		!strings.Contains(tx.lastSQL, "ON CONFLICT (id) DO NOTHING") {
		t.Fatalf("sql = %q, want the shared idempotent insert statement", tx.lastSQL)
	}
}

// 事务执行故障：原样包装上抛（调用方回滚业务事务）。
func TestTransactionalAppender_AppendTx_ExecFault_ReturnsWrappedError(t *testing.T) {
	d := testDriver(event.MigrateNone, &fakeManager{}, newFakeClock())
	ta := newTransactionalAppenderOn(d)
	boom := errors.New("exec boom")
	tx := &recordingTx{execErr: boom}

	err := ta.AppendTx(context.Background(), tx, event.RawEnvelope{
		ID: "ta-4", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
	})
	if !errors.Is(err, boom) {
		t.Fatalf("append tx = %v, want wrapped exec error", err)
	}
}

// 唤醒通道名派生（ADR 0013 §2）：短 schema 直用前缀 + 全名；超 Postgres
// 标识上限（63 字节）的 schema 走确定性哈希——恰 63 字节、同 schema 稳定、
// 异 schema 不同名，LISTEN 与 NOTIFY 两侧经同一函数派生必然一致。
func TestWakeChannel_LongSchema_DeterministicHash(t *testing.T) {
	if got := wakeChannel("public"); got != "go_event_wake_public" {
		t.Fatalf("short schema channel = %q, want direct name", got)
	}

	long := repeatSchema(56) // 前缀 14 字节 + 56 > 63：哈希路径
	ch := wakeChannel(long)
	if len(ch) > 63 {
		t.Fatalf("hashed channel = %d bytes, want within Postgres identifier limit (63)", len(ch))
	}
	if !strings.HasPrefix(ch, wakeChannelPrefix) {
		t.Fatalf("hashed channel = %q, want prefix %q", ch, wakeChannelPrefix)
	}
	if again := wakeChannel(long); again != ch {
		t.Fatalf("channel not deterministic: %q vs %q", ch, again)
	}
	if other := wakeChannel(repeatSchema(55)); other == ch {
		t.Fatal("distinct schemas must derive distinct channels")
	}
	// 截断边界：49 字节 schema（前缀 + 全名 = 63）仍是直名；50 字节起哈希。
	exact := repeatSchema(49)
	if got := wakeChannel(exact); got != wakeChannelPrefix+exact {
		t.Fatalf("63-byte channel = %q, want direct name", got)
	}
	if got := wakeChannel(repeatSchema(50)); got == wakeChannelPrefix+repeatSchema(50) {
		t.Fatal("64-byte channel must take the hash path")
	}
}

func repeatSchema(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
