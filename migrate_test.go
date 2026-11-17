package event_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jninng/go_event"
)

// 三种模式与未知值的字符串表示。
func TestMigrateModeString(t *testing.T) {
	tests := []struct {
		name string
		mode event.MigrateMode
		want string
	}{
		{"CreateMode_String_ReturnsCreate", event.MigrateCreate, "create"},
		{"VerifyMode_String_ReturnsVerify", event.MigrateVerify, "verify"},
		{"NoneMode_String_ReturnsNone", event.MigrateNone, "none"},
		{"UnknownMode_String_ReturnsNumeric", event.MigrateMode(42), "MigrateMode(42)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// 报告只在没有问题时为 OK。
func TestVerifyReportOK(t *testing.T) {
	t.Run("NoIssues_OK_ReturnsTrue", func(t *testing.T) {
		if !(event.VerifyReport{}).OK() {
			t.Fatal("OK() = false, want true for empty report")
		}
	})
	t.Run("OneIssue_OK_ReturnsFalse", func(t *testing.T) {
		report := event.VerifyReport{Issues: []event.VerifyIssue{{
			Object:  "public.event",
			Kind:    event.KindTable,
			Problem: event.ProblemMissing,
			Action:  "create table",
		}}}
		if report.OK() {
			t.Fatal("OK() = true, want false with one issue")
		}
	})
}

// VerifyError 可被 errors.As 取回并携带结构化报告。
func TestVerifyErrorErrorsAs(t *testing.T) {
	report := event.VerifyReport{Issues: []event.VerifyIssue{{
		Object:  "public.event",
		Kind:    event.KindTable,
		Problem: event.ProblemMissing,
		Action:  "create table",
	}}}
	var err error = &event.VerifyError{Report: report}
	var target *event.VerifyError
	if !errors.As(err, &target) {
		t.Fatal("errors.As(*VerifyError) = false, want true")
	}
	if len(target.Report.Issues) != 1 {
		t.Fatalf("report issues = %d, want 1", len(target.Report.Issues))
	}
	if got := target.Report.Issues[0].Object; got != "public.event" {
		t.Fatalf("issue object = %q, want %q", got, "public.event")
	}
}

// 嵌套包装后仍可 errors.As 取回。
func TestVerifyErrorWrappedErrorsAs(t *testing.T) {
	cause := &event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
		Object:  "public.event_pk",
		Kind:    event.KindConstraint,
		Problem: event.ProblemOutdated,
		Action:  "align constraint",
	}}}}
	err := fmt.Errorf("migrate: %w", cause)
	var target *event.VerifyError
	if !errors.As(err, &target) {
		t.Fatal("errors.As(*VerifyError) = false, want true through wrap")
	}
	if got := target.Report.Issues[0].Kind; got != event.KindConstraint {
		t.Fatalf("issue kind = %q, want %q", got, event.KindConstraint)
	}
}

// 能力声明与执行路径枚举的零值语义。
func TestMigrationCapabilitiesZeroValue(t *testing.T) {
	t.Run("ZeroValue_Atomicity_IsNone", func(t *testing.T) {
		var c event.MigrationCapabilities
		if c.Atomicity != event.DDLAtomicNone {
			t.Fatalf("Atomicity = %v, want DDLAtomicNone", c.Atomicity)
		}
	})
	t.Run("ZeroValue_Maps_AreNil", func(t *testing.T) {
		var c event.MigrationCapabilities
		if c.IfNotExists != nil || c.Probe != nil {
			t.Fatal("capability maps should be nil by default")
		}
	})
	t.Run("ZeroValue_ErrorClasses_AllFalse", func(t *testing.T) {
		var c event.MigrationCapabilities
		if c.Errors.Exists || c.Errors.Permission || c.Errors.Transient {
			t.Fatal("zero ErrorClasses should be all false")
		}
	})
	t.Run("ZeroValue_LockRelease_IsTransaction", func(t *testing.T) {
		var c event.MigrationCapabilities
		if c.Lock != event.LockNone {
			t.Fatalf("Lock = %v, want LockNone", c.Lock)
		}
		if c.LockRelease != event.LockReleaseTransaction {
			t.Fatalf("LockRelease = %v, want LockReleaseTransaction", c.LockRelease)
		}
		if c.Completion != event.DDLSync {
			t.Fatalf("Completion = %v, want DDLSync", c.Completion)
		}
	})
	t.Run("ZeroValue_ImplicitCommitAndNonTransactional_AreFalse", func(t *testing.T) {
		var c event.MigrationCapabilities
		if c.ImplicitCommit {
			t.Fatal("ImplicitCommit zero value = true, want false")
		}
		if c.NonTransactionalSteps {
			t.Fatal("NonTransactionalSteps zero value = true, want false")
		}
	})
}

// 永久错误判定覆盖迁移哨兵与差异错误，瞬时锁超时不算永久。
func TestIsPermanentMigrationError(t *testing.T) {
	verifyErr := &event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
		Object:  "public.event",
		Kind:    event.KindTable,
		Problem: event.ProblemMissing,
	}}}}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"NilError_IsPermanent_ReturnsFalse", nil, false},
		{"VerifyError_IsPermanent_ReturnsTrue", verifyErr, true},
		{"WrappedVerifyError_IsPermanent_ReturnsTrue", fmt.Errorf("migrate: %w", verifyErr), true},
		{"MigrationUnsupported_IsPermanent_ReturnsTrue", event.ErrMigrationUnsupported, true},
		{"MigrateModeUnsupported_IsPermanent_ReturnsTrue", event.ErrMigrateModeUnsupported, true},
		{"PermissionDenied_IsPermanent_ReturnsTrue",
			fmt.Errorf("ddl: %w", event.ErrMigrationPermissionDenied), true},
		{"InvalidSchema_IsPermanent_ReturnsTrue",
			fmt.Errorf("schema: %w: bad", event.ErrInvalidSchema), true},
		{"LockTimeout_IsPermanent_ReturnsFalse", event.ErrMigrationLockTimeout, false},
		{"GenericError_IsPermanent_ReturnsFalse", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := event.IsPermanentMigrationError(tt.err); got != tt.want {
				t.Fatalf("IsPermanentMigrationError() = %v, want %v", got, tt.want)
			}
		})
	}
}
