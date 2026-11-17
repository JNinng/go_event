package postgrec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// TransactionalAppender 入口语义单测（无 DB，注入替身）：ensure 失败上抛
// 且不触碰调用方事务；取消在 ensure 之前短路。database/sql 的 *sql.Tx 为
// 具体类型无法替身，事务执行路径与提交后可见性由 TEST_POSTGRES_DSN 门控
// 的契约套件与集成测试覆盖——nil tx 仅在确保不触达的失败路径上作为占位。

// ensure 失败（迁移面错误）：原样上抛，调用方事务零触碰（事务控制归调用方）。
func TestTransactionalAppender_AppendTx_EnsureFailure_ReturnsErrorWithoutTouchingTx(t *testing.T) {
	boom := errors.New("boom migrate")
	d := testDriver(event.MigrateCreate, &fakeManager{migrateErr: boom}, newFakeClock())
	ta := newTransactionalAppenderOn(d)

	// nil tx 占位：本路径在 tx.Exec 之前返回，绝不触碰事务（若实现错误地
	// 先触碰事务将以 panic 暴露，测试即失败）。
	err := ta.AppendTx(context.Background(), nil, event.RawEnvelope{
		ID: "ta-1", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
	})
	if !errors.Is(err, boom) {
		t.Fatalf("append tx = %v, want ensure error", err)
	}
}

// 调用方取消：在 ensure 之前短路，不触碰迁移面与调用方事务。
func TestTransactionalAppender_AppendTx_CanceledContext_ShortCircuitsBeforeEnsure(t *testing.T) {
	mgr := &fakeManager{}
	d := testDriver(event.MigrateCreate, mgr, newFakeClock())
	ta := newTransactionalAppenderOn(d)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer cancel()
	err := ta.AppendTx(ctx, nil, event.RawEnvelope{
		ID: "ta-2", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("append tx = %v, want context.Canceled", err)
	}
	if mgr.migrateCalls.Load() != 0 {
		t.Fatalf("migrate calls = %d, want 0 (canceled ctx must short-circuit)", mgr.migrateCalls.Load())
	}
}
