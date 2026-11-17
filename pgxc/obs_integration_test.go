package pgxc_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/pgxc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// 迁移生命周期指标集成测试（TEST_POSTGRES_DSN 门控）：Migrate/Verify 与
// driver ensure 走同一发射点，ManualReader 收集断言。

type captureHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.Contains(m, msg) {
			return true
		}
	}
	return false
}

// migrateDatapoint 收集 result 属性匹配的 pgxc.migrate 数据点值总和
// （指标未出现返回 0）。
func migrateDatapoint(t *testing.T, reader *sdkmetric.ManualReader, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "pgxc.migrate" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric pgxc.migrate type %T, want Sum[int64]", m.Data)
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

func TestMigrateObservability_Migrate_CountsOKAndLogsApplied(t *testing.T) {
	pool := migrationPool(t)
	schema := newMigrationSchema(t, pool)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	logs := &captureHandler{}

	if err := pgxc.Migrate(context.Background(), pool,
		pgxc.WithSchema(schema), pgxc.WithMeterProvider(mp), pgxc.WithLogger(slog.New(logs))); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if got := migrateDatapoint(t, reader, "ok"); got != 1 {
		t.Fatalf("migrate ok count = %d, want 1", got)
	}
	if !logs.has("pgxc: migration applied") {
		t.Fatal("migration applied log missing")
	}
}

func TestMigrateObservability_Conflict_CountsVerifyConflict(t *testing.T) {
	pool := migrationPool(t)
	ctx := context.Background()
	schema := newMigrationSchema(t, pool)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE TYPE "+schema+".event_claim AS ENUM ('x')"); err != nil {
		t.Fatalf("create conflicting type: %v", err)
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	err := pgxc.Migrate(ctx, pool, pgxc.WithSchema(schema), pgxc.WithMeterProvider(mp))
	var verifyErr *event.VerifyError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("migrate = %v, want *VerifyError", err)
	}
	if got := migrateDatapoint(t, reader, "verify_conflict"); got != 1 {
		t.Fatalf("migrate verify_conflict count = %d, want 1", got)
	}
}

func TestDriverEnsure_EmitsMigrationMetricOnFirstOperation(t *testing.T) {
	pool := migrationPool(t)
	schema := newMigrationSchema(t, pool)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	d := pgxc.NewDriver(pool, pgxc.WithSchema(schema), pgxc.WithMeterProvider(mp))
	if _, err := d.HeadPosition(context.Background(), "any"); err != nil {
		t.Fatalf("head position: %v", err)
	}
	if got := migrateDatapoint(t, reader, "ok"); got != 1 {
		t.Fatalf("migrate ok count = %d, want 1 (single emission point)", got)
	}
}
