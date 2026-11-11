package postgrec_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 驱动名 "pgx"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/postgrec"
)

// 集成测试需真实 Postgres：设置 TEST_POSTGRES_DSN（如
// postgres://user:pass@localhost:5432/postgres）启用；未设置时跳过。
func testDB(t *testing.T) (*sql.DB, event.Driver) {
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
	return db, drv
}

// 幂等 DDL：可重复执行（design.md §8）。
func TestMigrateIdempotent(t *testing.T) {
	db, drv := testDB(t)
	ctx := context.Background()
	schema := "go_event_migrate_" + fmt.Sprint(time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 1: %v", err)
	}
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 2 (must be idempotent): %v", err)
	}
	// NewDriver 自动初始化亦幂等。
	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate 3: %v", err)
	}
	_ = drv
}

// 全套契约测试（语义即测试，design.md §9.1）。
func TestPostgrecContractSuite(t *testing.T) {
	_, drv := testDB(t)
	eventtest.RunSuite(t, drv)
}

// 保留窗口惰性清理契约：每用例独立 schema + 短保留窗口的全新 Driver
// （design.md §5.2）。
func TestPostgrecRetentionSuite(t *testing.T) {
	db, _ := testDB(t)
	var seq atomic.Int64
	eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
		schema := fmt.Sprintf("go_event_ret_%d_%d", time.Now().UnixNano(), seq.Add(1))
		t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
		return postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithRetention(2*time.Second))
	})
}
