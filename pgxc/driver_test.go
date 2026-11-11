package pgxc_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/pgxc"
)

// 集成测试需真实 Postgres：设置 TEST_POSTGRES_DSN（如
// postgres://user:pass@localhost:5432/postgres）启用；未设置时跳过。
func testPool(t *testing.T) (*pgxpool.Pool, event.Driver) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skip Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping db: %v", err)
	}
	t.Cleanup(pool.Close)
	// 每次运行独立 schema：契约测试的类型隔离 + 运行间互不残留。
	schema := fmt.Sprintf("go_event_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})
	drv := pgxc.NewDriver(pool, pgxc.WithSchema(schema),
		pgxc.WithRetention(7*24*time.Hour))
	return pool, drv
}

// 幂等 DDL：可重复执行（design.md §8）。
func TestMigrateIdempotent(t *testing.T) {
	pool, _ := testPool(t)
	ctx := context.Background()
	schema := "go_event_migrate_" + fmt.Sprint(time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})
	if err := pgxc.Migrate(ctx, pool, pgxc.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 1: %v", err)
	}
	if err := pgxc.Migrate(ctx, pool, pgxc.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 2 (must be idempotent): %v", err)
	}
	if err := pgxc.Migrate(ctx, pool, pgxc.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 3: %v", err)
	}
}

// 全套契约测试（语义即测试，design.md §9.1）。
func TestPgxcContractSuite(t *testing.T) {
	_, drv := testPool(t)
	eventtest.RunSuite(t, drv)
}

// 保留窗口惰性清理契约：每用例独立 schema + 短保留窗口的全新 Driver
// （design.md §5.2）。
func TestPgxcRetentionSuite(t *testing.T) {
	pool, _ := testPool(t)
	var seq atomic.Int64
	eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
		schema := fmt.Sprintf("go_event_ret_%d_%d", time.Now().UnixNano(), seq.Add(1))
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		})
		return pgxc.NewDriver(pool, pgxc.WithSchema(schema),
			pgxc.WithRetention(2*time.Second))
	})
}
