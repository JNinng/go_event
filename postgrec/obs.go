package postgrec

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jninng/go_event"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// defaultScope 是指标 scope 与指标名前缀的缺省值（模块名）。
const defaultScope = "postgrec"

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

// adminMetrics 是管理面写操作仪表集（发射点单一：admin 写方法）。属性
// 只有枚举与标识符（result / schema），事件 ID 与错误原文禁入。
type adminMetrics struct {
	replay         metric.Int64Counter
	replayDuration metric.Float64Histogram
}

func newAdminMetrics(mp metric.MeterProvider, scope string) *adminMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider() // 兜底：未配置时即 noop provider
	}
	m := mp.Meter(scope)
	am := &adminMetrics{}
	am.replay, _ = m.Int64Counter(scope+".admin.replay",
		metric.WithDescription("管理面重放结果计数"), metric.WithUnit("1"))
	am.replayDuration, _ = m.Float64Histogram(scope+".admin.replay.duration",
		metric.WithDescription("管理面重放耗时（含 ensure、锁等待与事务执行）"), metric.WithUnit("s"))
	return am
}

// adminResult 管理面重放结果枚举（值即 event 的 Admin 重放结果枚举）。
func adminResult(ok bool, err error) string {
	switch {
	case err == nil && ok:
		return event.AdminReplayOK
	case err == nil:
		return event.AdminReplayNoEffect
	case errors.Is(err, context.Canceled):
		return event.AdminReplayCanceled
	case errors.Is(err, event.ErrAdminLockTimeout):
		return event.AdminReplayLockTimeout
	default:
		return event.AdminReplayError
	}
}

// record 计量一次管理面重放（ctx 携带 trace 关联）：计数带 result 与
// schema 属性；耗时直方图只带 result（与迁移耗时同口径，schema 不入
// 耗时维度，控制基数）。
func (am *adminMetrics) record(ctx context.Context, schema string, ok bool, err error, elapsed time.Duration) {
	res := adminResult(ok, err)
	am.replay.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrResult, res),
		attribute.String(attrSchema, schema)))
	am.replayDuration.Record(ctx, elapsed.Seconds(),
		metric.WithAttributes(attribute.String(attrResult, res)))
}

// observeAdminReplay 管理面重放观测旁路分发：经 ObserverV3 类型断言，
// 未实现 V3 的观察者不感知管理面事件；回调 recover panic，纯旁路。
func observeAdminReplay(o event.Observer, ev event.AdminReplayEvent) {
	v3, ok := o.(event.ObserverV3)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	v3.OnAdminReplay(ev)
}

// logErrAttrs 日志错误属性（OTel 语义约定：error.type 类型名 + error
// 字符串化，不携带堆栈）。
func logErrAttrs(err error) []any {
	return []any{"error.type", fmt.Sprintf("%T", err), "error", err.Error()}
}
