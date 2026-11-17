package pgxc

import (
	"context"
	"errors"
	"time"

	"github.com/jninng/go_event"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// defaultScope 是指标 scope 与指标名前缀的缺省值（模块名）。
const defaultScope = "pgxc"

// 迁移结果枚举与属性键（OTel 语义约定，本地常量固定；属性值仅枚举与
// 标识符，schema 属部署期标识符）。
const (
	migResultOK             = "ok"
	migResultCanceled       = "canceled"
	migResultLockTimeout    = "lock_timeout"
	migResultPermission     = "permission_denied"
	migResultVerifyConflict = "verify_conflict"
	migResultPermanent      = "permanent"
	migResultTransient      = "transient"

	attrSchema = "schema"
	attrMode   = "mode"
	attrResult = "result"
)

// migrationMetrics 是迁移生命周期仪表集。发射点单一：包级 Migrate/Verify
// （driver ensure 经 pgSchemaManager 走同一函数，全路径无重复计量）。
type migrationMetrics struct {
	migrate         metric.Int64Counter
	migrateDuration metric.Float64Histogram
}

func newMigrationMetrics(mp metric.MeterProvider, scope string) *migrationMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider() // 兜底：未配置时即 noop provider
	}
	m := mp.Meter(scope)
	mm := &migrationMetrics{}
	mm.migrate, _ = m.Int64Counter(scope+".migrate",
		metric.WithDescription("迁移/校验结果计数"), metric.WithUnit("1"))
	mm.migrateDuration, _ = m.Float64Histogram(scope+".migrate.duration",
		metric.WithDescription("迁移/校验耗时"), metric.WithUnit("s"))
	return mm
}

// migrationResult 迁移结果枚举：锁超时/权限/定义冲突单列（处置动作不同），
// 其余按 IsPermanentMigrationError 归入 permanent/transient。
func migrationResult(err error) string {
	switch {
	case err == nil:
		return migResultOK
	case errors.Is(err, context.Canceled):
		return migResultCanceled
	case errors.Is(err, event.ErrMigrationLockTimeout):
		return migResultLockTimeout
	case errors.Is(err, event.ErrMigrationPermissionDenied):
		return migResultPermission
	default:
		var verr *event.VerifyError
		if errors.As(err, &verr) {
			return migResultVerifyConflict
		}
		if event.IsPermanentMigrationError(err) {
			return migResultPermanent
		}
		return migResultTransient
	}
}

// record 计量一次迁移/校验（ctx 携带 trace 关联）。
func (mm *migrationMetrics) record(ctx context.Context, scope, schema string,
	err error, elapsed time.Duration) {
	res := migrationResult(err)
	attrs := metric.WithAttributes(
		attribute.String(attrResult, res),
		attribute.String(attrSchema, schema))
	mm.migrate.Add(ctx, 1, attrs)
	mm.migrateDuration.Record(ctx, elapsed.Seconds(),
		metric.WithAttributes(attribute.String(attrResult, res)))
}
