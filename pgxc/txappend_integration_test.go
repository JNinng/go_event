package pgxc_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/pgxc"
)

// 事务性入队（outbox 可选面，ADR 0012）集成测试（TEST_POSTGRES_DSN 门控）：
// 契约套件经 TxSession 适配统一跑（inmem 同套件，无需按后端门控）；另钉
// outbox 端到端原子性——业务表写入与事件入队共用同一事务，提交/回滚
// 同进退（业务表由测试自建，验证调用方业务事务载体语义）。

// pgxTxSession 把 pgx.Tx 适配成套件 TxSession：同一事务承载全部调用。
type pgxTxSession struct {
	ta *pgxc.TransactionalAppender
	tx pgx.Tx
}

func (s pgxTxSession) AppendTx(t *testing.T, e event.RawEnvelope) {
	t.Helper()
	if err := s.ta.AppendTx(t.Context(), s.tx, e); err != nil {
		t.Fatalf("append tx: %v", err)
	}
}

func (s pgxTxSession) Commit(t *testing.T) {
	t.Helper()
	if err := s.tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}

func (s pgxTxSession) Rollback(t *testing.T) {
	t.Helper()
	if err := s.tx.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
}

// 契约套件（回滚零事件、中途回滚连带丢弃、提交前不可见、提交后可见、
// 事务内幂等、回滚缺口、乱序边界）。
func TestPgxcTransactionalAppendSuite(t *testing.T) {
	pool, drv, schema := testPool(t)
	ta := pgxc.NewTransactionalAppender(pool, pgxc.WithSchema(schema))
	eventtest.RunTransactionalAppendSuite(t, drv, func(t *testing.T) eventtest.TxSession {
		tx, err := pool.Begin(t.Context())
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		return pgxTxSession{ta: ta, tx: tx}
	})
}

// outbox 端到端原子性：业务写与 AppendTx 共用同一业务事务。
func TestPgxcOutboxBusinessWriteAndAppendShareTx(t *testing.T) {
	pool, drv, schema := testPool(t)
	ta := pgxc.NewTransactionalAppender(pool, pgxc.WithSchema(schema))
	ctx := context.Background()

	table := schema + ".outbox_order"
	// 业务表建立在事件库 schema 之后：schema 由首次 Driver 操作惰性创建，
	// 先显式迁移再建业务表（避免测试对惰性时序的依赖）。
	if err := pgxc.Migrate(ctx, pool, pgxc.WithSchema(schema)); err != nil {
		t.Fatalf("provision event schema: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE `+table+` (id text PRIMARY KEY)`); err != nil {
		t.Fatalf("create business table: %v", err)
	}

	envelope := func(id string) event.RawEnvelope {
		return event.RawEnvelope{
			ID: id, Type: "outbox.order", Payload: []byte(`{"order":"o-1"}`),
			OccurredAt: time.Now().UTC(),
		}
	}
	businessRowCount := func(t *testing.T, id string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM `+table+` WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("count business rows: %v", err)
		}
		return n
	}

	t.Run("CommittedTx_BusinessWriteAndAppendTx_BothVisible", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+table+` (id) VALUES ($1)`, "ob-c-1"); err != nil {
			t.Fatalf("business insert: %v", err)
		}
		if err := ta.AppendTx(ctx, tx, envelope("ob-c-1")); err != nil {
			t.Fatalf("append tx: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit tx: %v", err)
		}

		if n := businessRowCount(t, "ob-c-1"); n != 1 {
			t.Fatalf("business rows = %d, want 1 after commit", n)
		}
		envs, err := drv.Fetch(ctx, event.FetchQuery{Type: "outbox.order", Limit: 10})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if len(envs) != 1 || envs[0].ID != "ob-c-1" {
			t.Fatalf("fetch = %+v, want exactly ob-c-1 (event committed with the business tx)", envs)
		}
	})

	t.Run("RolledBackTx_BusinessWriteAndAppendTx_BothDiscarded", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+table+` (id) VALUES ($1)`, "ob-r-1"); err != nil {
			t.Fatalf("business insert: %v", err)
		}
		if err := ta.AppendTx(ctx, tx, envelope("ob-r-1")); err != nil {
			t.Fatalf("append tx: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback tx: %v", err)
		}

		if n := businessRowCount(t, "ob-r-1"); n != 0 {
			t.Fatalf("business rows = %d, want 0 after rollback", n)
		}
		envs, err := drv.Fetch(ctx, event.FetchQuery{Type: "outbox.order", Limit: 10})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		for _, e := range envs {
			if e.ID == "ob-r-1" {
				t.Fatal("rolled-back event must not be visible")
			}
		}
	})
}

// Store 聚合的事务性入队列与独立构造同源（同一连接池与命名空间）：
// Store.TransactionalAppender 写入的事件对 Store.Driver 立即可见（提交后）。
func TestPgxcStoreTransactionalAppender_AggregatesSameOrigin(t *testing.T) {
	pool, _, schema := testPool(t)
	st := pgxc.NewStore(pool, pgxc.WithSchema(schema))
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := st.TransactionalAppender.AppendTx(ctx, tx, event.RawEnvelope{
		ID: "st-ta-1", Type: t.Name(), Payload: []byte(`{}`), OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("append tx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	envs, err := st.Driver.Fetch(ctx, event.FetchQuery{Type: t.Name(), Limit: 10})
	if err != nil {
		t.Fatalf("fetch via store driver: %v", err)
	}
	if len(envs) != 1 || envs[0].ID != "st-ta-1" {
		t.Fatalf("fetch = %+v, want exactly st-ta-1 (store surfaces share origin)", envs)
	}
}
