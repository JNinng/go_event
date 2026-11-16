package inmem_test

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/eventtest/inmem"
)

// 进程内 Driver 无持久结构：迁移模式固定为 no-op，不参与 RunMigrationSuite
// 的建表用例，这里固定其 no-op 语义。
func TestInmemMigrationNoop(t *testing.T) {
	t.Run("Capabilities_Declared_ZeroValue", func(t *testing.T) {
		cap := inmem.Capabilities()
		if cap.Atomicity != event.DDLAtomicNone {
			t.Fatalf("Atomicity = %v, want DDLAtomicNone", cap.Atomicity)
		}
		if cap.Lock != event.LockNone {
			t.Fatalf("Lock = %v, want LockNone", cap.Lock)
		}
		if cap.ImplicitCommit || cap.NonTransactionalSteps {
			t.Fatalf("capabilities = %+v, want zero-value no-op declaration", cap)
		}
	})
	t.Run("Empty_Migrate_ReturnsNil", func(t *testing.T) {
		if err := inmem.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate = %v, want nil", err)
		}
	})
	t.Run("Empty_Verify_ReportsOK", func(t *testing.T) {
		report, err := inmem.Verify(t.Context())
		if err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
		if !report.OK() {
			t.Fatalf("Verify issues = %+v, want empty", report.Issues)
		}
	})
	t.Run("ExistingData_MigrateAndVerify_NoStateChange", func(t *testing.T) {
		drv := inmem.New()
		env := event.RawEnvelope{
			ID: "noop-1", Type: "noop", Payload: []byte(`{}`), OccurredAt: time.Now(),
		}
		if err := drv.Append(t.Context(), env); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := inmem.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate = %v, want nil", err)
		}
		if _, err := inmem.Verify(t.Context()); err != nil {
			t.Fatalf("Verify = %v, want nil", err)
		}
		batch, err := drv.Fetch(t.Context(), event.FetchQuery{Type: "noop", Limit: 10})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if len(batch) != 1 || batch[0].ID != env.ID {
			t.Fatalf("migration no-op changed state: %+v", batch)
		}
	})
}

// inmem Driver 必须全过契约套件（语义即测试，design.md §9.1）。
func TestInmemContractSuite(t *testing.T) {
	eventtest.RunSuite(t, inmem.New())
}

// 保留窗口惰性清理契约：每用例独立的短窗口全新 Driver（design.md §5.2）。
func TestInmemRetentionSuite(t *testing.T) {
	eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
		return inmem.New(inmem.WithRetention(2 * time.Second))
	})
}
