package postgrec

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jninng/go_event"
)

// migrationResult 分类单元测试：迁移生命周期指标的 result 枚举是观测
// 契约，逐档断言（锁超时/权限/定义冲突单列，其余按永久/瞬时归类）。
func TestMigrationResult_ClassifyByErrorKind(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"NilError_MigrationResult_ReturnsOK", nil, migResultOK},
		{"ContextCanceled_MigrationResult_ReturnsCanceled", context.Canceled, migResultCanceled},
		{"LockTimeout_MigrationResult_ReturnsLockTimeout",
			fmt.Errorf("postgrec: migrate: %w", event.ErrMigrationLockTimeout), migResultLockTimeout},
		{"PermissionDenied_MigrationResult_ReturnsPermission",
			fmt.Errorf("postgrec: migrate: %w", event.ErrMigrationPermissionDenied), migResultPermission},
		{"VerifyConflict_MigrationResult_ReturnsVerifyConflict",
			&event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
				Object: "s.event", Kind: event.KindTable, Problem: event.ProblemOutdated,
			}}}}, migResultVerifyConflict},
		{"InvalidSchema_MigrationResult_ReturnsPermanent",
			fmt.Errorf("postgrec: migrate: %w: %v", event.ErrInvalidSchema, errors.New("bad")), migResultPermanent},
		{"ModeUnsupported_MigrationResult_ReturnsPermanent",
			fmt.Errorf("postgrec: migrate: %w", event.ErrMigrateModeUnsupported), migResultPermanent},
		{"PlainError_MigrationResult_ReturnsTransient", errors.New("conn refused"), migResultTransient},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := migrationResult(tt.err); got != tt.want {
				t.Fatalf("migrationResult(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}
