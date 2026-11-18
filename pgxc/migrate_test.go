package pgxc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/pgxc"
)

// 能力声明是契约测试读取的输入，字段必须精确。
func TestCapabilities(t *testing.T) {
	t.Run("TransactionalAtomicity_Declared", func(t *testing.T) {
		if got := pgxc.Capabilities().Atomicity; got != event.DDLAtomicTransactional {
			t.Fatalf("Atomicity = %v, want DDLAtomicTransactional", got)
		}
	})
	t.Run("IfNotExists_PerObjectKind", func(t *testing.T) {
		got := pgxc.Capabilities().IfNotExists
		want := map[event.ObjectKind]event.NativeSupport{
			event.KindSchema:     event.SupportNative,
			event.KindTable:      event.SupportNative,
			event.KindColumn:     event.SupportNative,
			event.KindIndex:      event.SupportNative,
			event.KindConstraint: event.SupportNone,
		}
		for kind, support := range want {
			if got[kind] != support {
				t.Fatalf("IfNotExists[%s] = %v, want %v", kind, got[kind], support)
			}
		}
	})
	t.Run("Probe_PerObjectKind", func(t *testing.T) {
		got := pgxc.Capabilities().Probe
		for _, kind := range []event.ObjectKind{
			event.KindTable, event.KindColumn, event.KindIndex, event.KindConstraint,
		} {
			if got[kind] != event.ProbeDefinition {
				t.Fatalf("Probe[%s] = %v, want ProbeDefinition", kind, got[kind])
			}
		}
		if got[event.KindSchema] != event.ProbeExistence {
			t.Fatalf("Probe[schema] = %v, want ProbeExistence", got[event.KindSchema])
		}
	})
	t.Run("ErrorClasses_AllRecognized", func(t *testing.T) {
		got := pgxc.Capabilities().Errors
		if !got.Exists || !got.Permission || !got.Transient {
			t.Fatalf("Errors = %+v, want all true", got)
		}
	})
	t.Run("LockAndCompletion_Declared", func(t *testing.T) {
		cap := pgxc.Capabilities()
		if cap.Lock != event.LockTransaction {
			t.Fatalf("Lock = %v, want LockTransaction", cap.Lock)
		}
		if cap.LockRelease != event.LockReleaseTransaction {
			t.Fatalf("LockRelease = %v, want LockReleaseTransaction", cap.LockRelease)
		}
		if cap.Completion != event.DDLSync {
			t.Fatalf("Completion = %v, want DDLSync", cap.Completion)
		}
	})
	t.Run("TransactionSemantics_Declared", func(t *testing.T) {
		cap := pgxc.Capabilities()
		if cap.ImplicitCommit {
			t.Fatal("ImplicitCommit = true, want false (PG DDL is transactional)")
		}
		if !cap.NonTransactionalSteps {
			t.Fatal("NonTransactionalSteps = false, want true (plans may declare CONCURRENTLY)")
		}
	})
}

// WithMigrateMode 对 NewDriver / NewStore（内部 Driver 构造期）生效：
// 显式传给 Migrate 返回哨兵错误。
func TestMigrateWithModeOption(t *testing.T) {
	t.Run("ExplicitMode_Migrate_ReturnsUnsupported", func(t *testing.T) {
		// 模式检查发生在任何 DB 访问之前，nil *pgxpool.Pool 是安全的。
		err := pgxc.Migrate(context.Background(), nil,
			pgxc.WithMigrateMode(event.MigrateNone))
		if !errors.Is(err, event.ErrMigrateModeUnsupported) {
			t.Fatalf("Migrate = %v, want ErrMigrateModeUnsupported", err)
		}
	})
	t.Run("InvalidSchema_Migrate_ReturnsErrInvalidSchema", func(t *testing.T) {
		err := pgxc.Migrate(context.Background(), nil, pgxc.WithSchema("bad-name"))
		if !errors.Is(err, event.ErrInvalidSchema) {
			t.Fatalf("Migrate = %v, want ErrInvalidSchema", err)
		}
	})
	t.Run("InvalidSchema_Verify_ReturnsErrInvalidSchema", func(t *testing.T) {
		_, err := pgxc.Verify(context.Background(), nil, pgxc.WithSchema("bad-name"))
		if !errors.Is(err, event.ErrInvalidSchema) {
			t.Fatalf("Verify = %v, want ErrInvalidSchema", err)
		}
	})
}

// 导出物默认使用 public schema 且可重复构造。
func TestStatementsAndSQL(t *testing.T) {
	t.Run("DefaultOptions_Statements_NonEmptyWithoutLock", func(t *testing.T) {
		stmts, err := pgxc.Statements()
		if err != nil {
			t.Fatalf("Statements: %v", err)
		}
		if len(stmts) == 0 {
			t.Fatal("Statements = empty, want baseline")
		}
	})
	t.Run("DefaultOptions_SQL_StartsWithBegin", func(t *testing.T) {
		script, err := pgxc.SQL()
		if err != nil {
			t.Fatalf("SQL: %v", err)
		}
		if len(script) < len("BEGIN;") || script[:len("BEGIN;")] != "BEGIN;" {
			t.Fatalf("SQL = %q, want BEGIN; prefix", script)
		}
	})
	t.Run("InvalidSchema_StatementsAndSQL_ReturnError", func(t *testing.T) {
		if _, err := pgxc.Statements(pgxc.WithSchema("bad-name")); err == nil {
			t.Fatal("Statements(invalid) = nil error, want error")
		}
		if _, err := pgxc.SQL(pgxc.WithSchema("bad-name")); err == nil {
			t.Fatal("SQL(invalid) = nil error, want error")
		}
	})
}

// NewDriver 在所有模式下都校验 schema，非法名不会进入任何 DML/DDL。
func TestNewDriverInvalidSchema(t *testing.T) {
	for _, mode := range []event.MigrateMode{event.MigrateCreate, event.MigrateVerify, event.MigrateNone} {
		t.Run(mode.String()+"Mode_InvalidSchemaAppend_ReturnsErrInvalidSchema", func(t *testing.T) {
			drv := pgxc.NewDriver(nil, pgxc.WithSchema("bad-name"),
				pgxc.WithMigrateMode(mode))
			err := drv.Append(context.Background(), event.RawEnvelope{
				ID: "x", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
			})
			if !errors.Is(err, event.ErrInvalidSchema) {
				t.Fatalf("Append = %v, want ErrInvalidSchema", err)
			}
			if !event.IsPermanentMigrationError(err) {
				t.Fatal("invalid schema must be classified permanent")
			}
		})
	}
}
