package event

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// 可观测规范（otel + slog）：指标走 otel 全局兜底，选项优先；热路径只
// 打点不打日志；观测（回调 + 指标 + 日志）均为旁路。接入说明见
// docs/api.md §2.9 与 docs/adr/0008。

const (
	// defaultScope 是指标 scope 与指标名前缀的缺省值（本库库名）。
	defaultScope = "go_event"

	// 属性键对齐 OTel 语义约定（snake_case，本地常量固定）；属性值只允许
	// 枚举与标识符，业务 payload（事件 ID、错误原文）禁入。
	attrEvent    = "event"
	attrMode     = "mode"
	attrResult   = "result"
	attrIdentity = "identity"
)

// result 枚举值（属性 result 的取值）。
const (
	resultOK           = "ok"
	resultInvalidID    = "invalid_id"    // WithEventID 空串，未发起 Append
	resultEncodeError  = "encode_error"  // codec 编码失败，未发起 Append
	resultAppendError  = "append_error"  // Driver.Append 失败
	resultError        = "error"         // 轮询/终态操作失败
	resultCanceled     = "canceled"      // 调用方取消（停机路径，不记错误日志）
	resultHandlerError = "handler_error" // handler 返回错误
	resultHandlerPanic = "handler_panic" // handler panic（已转 error，原路径不变）
	resultSent         = "sent"          // 唤醒信号全部送达
	resultDropped      = "dropped"       // 唤醒信号至少一个订阅通道满被丢弃
)

// mode 属性取值（消费方式）。
const (
	modeNameBroadcast = "broadcast"
	modeNameClaim     = "claim"
)

// PublishEvent 是一次发布的观测事件（Result 取 result 枚举值；Err 仅在
// 非 ok 时非 nil）。
type PublishEvent struct {
	Event  string
	Result string
	Err    error
}

// ConsumeEvent 是一次 handler 消费的观测事件（Attempts 为认领侧尝试
// 次数，广播恒为 0）。
type ConsumeEvent struct {
	Mode     string
	Event    string
	Result   string
	Attempts int
	Err      error
}

// WakeEvent 是一次就近唤醒信号的观测事件（ADR 0003：仅信号不承载投递
// 保证，丢弃由周期 poll 兜底）。
type WakeEvent struct {
	Event    string
	Notified int // 送达的订阅通道数
	Dropped  int // 因通道满被丢弃的订阅通道数
}

// LeaseEvent 是停机放弃租约（到期由其他实例接管，at-least-once 不破坏）
// 的观测事件。
type LeaseEvent struct {
	Event string
}

// ClaimEvent 是一次认领轮的观测事件：发放与接管/终态计数随 Driver.Claim
// 的 ClaimResult 返回，与落库事实一致（design.md §9.2）。仅成功轮次
// 分发（失败轮次经 <scope>.claim 指标的 result=error 与日志可见）；
// 空轮次（零发放）同样分发——与指标侧「计数非零才打点」不对称，
// 观察者以事件为轮级事实流、以指标为聚合口径。
type ClaimEvent struct {
	Event          string
	Identity       string
	Leases         int // 本轮发放租约数
	Takeovers      int // 本轮接管发放数量（含终态接管）
	TerminalFailed int // 本轮认领动作直接置 failed 的数量
}

// StopEvent 是 Runner 停机完成的观测事件；TimedOut 表示等待在途 handler
// 超过调用方 ctx（在途 handler 不受影响，继续跑完）。
type StopEvent struct {
	TimedOut bool
}

// Admin 重放结果枚举（AdminReplayEvent.Result 与后端管理面指标 result
// 属性的取值）。ok / no_effect 之外均为失败返回：canceled 是调用方取消
// （不记错误日志），lock_timeout 是命名空间互斥锁等待超时（瞬时，可
// 重试），error 是其余执行故障（连接、迁移面等）。
const (
	AdminReplayOK          = "ok"           // 重放生效（行原为 failed 且已重放）
	AdminReplayNoEffect    = "no_effect"    // 无效果（ok=false 且无错误）
	AdminReplayCanceled    = "canceled"     // 调用方取消
	AdminReplayLockTimeout = "lock_timeout" // 互斥锁等待超时（瞬时）
	AdminReplayError       = "error"        // 执行故障
)

// AdminReplayEvent 是一次管理面重放的观测事件（ADR 0010：管理写操作记录
// 操作来源）。EventID × ConsumerType 是重放对象（事件 × 消费者身份，仅
// 标识不入指标属性）；Result 取 Admin 重放结果枚举值；Err 仅在失败结果
// （canceled / lock_timeout / error）时非 nil。
type AdminReplayEvent struct {
	EventID      string
	ConsumerType string
	Result       string
	Err          error
}

// Observer 观测旁路接口：回调在调用方 goroutine 同步执行，须微秒级返回；
// 库内 recover panic，不改变业务语义。
type Observer interface {
	OnPublish(PublishEvent)
	OnConsume(ConsumeEvent)
	OnWake(WakeEvent)
}

// ObserverV2 可选扩展接口：嵌入 Observer 非破坏性演进，分发点类型断言。
type ObserverV2 interface {
	Observer
	OnLeaseAbandoned(LeaseEvent)
	OnRunnerStopped(StopEvent)
}

// ObserverV3 可选扩展接口：管理面写操作观测（嵌入 ObserverV2 同款非破坏
// 性演进，分发点类型断言——未实现 V3 的观察者不感知管理面事件）。
type ObserverV3 interface {
	ObserverV2
	OnAdminReplay(AdminReplayEvent)
}

// ObserverV4 可选扩展接口：认领轮接管与终态计数观测（嵌入 ObserverV3
// 同款非破坏性演进，分发点类型断言——未实现 V4 的观察者不感知认领轮
// 事件）。对应指标为 <scope>.claim.takeover 与 <scope>.claim.terminal_failed。
type ObserverV4 interface {
	ObserverV3
	OnClaim(ClaimEvent)
}

type meterOption struct{ mp metric.MeterProvider }

func (o meterOption) applyPublisher(p *Publisher) { p.mp = o.mp }
func (o meterOption) applyRunner(r *Runner)       { r.mp = o.mp }

// WithMeterProvider 注入指标 MeterProvider（而非 Meter；库内部以自身
// scope 派生 meter 并注册仪表）。缺省在构造期快照 otel.GetMeterProvider()
// ——未装配时即 noop provider，天然静默；快照后替换全局不追溯已建实例。
func WithMeterProvider(mp metric.MeterProvider) meterOption { return meterOption{mp} }

type scopeOption struct{ s string }

func (o scopeOption) applyPublisher(p *Publisher) { p.scope = o.s }
func (o scopeOption) applyRunner(r *Runner)       { r.scope = o.s }

// WithScopeName 覆写指标 scope 与指标名前缀（<scope>.<动作>）；缺省为
// 库名 go_event。部署期决策——改名会使仪表盘口径漂移；同一进程内区分
// 多实例请用维度属性（枚举/标识符），勿用 scope。
func WithScopeName(scope string) scopeOption { return scopeOption{scope} }

type observerOption struct{ o Observer }

func (o observerOption) applyPublisher(p *Publisher) { p.obs = o.o }
func (o observerOption) applyRunner(r *Runner)       { r.obs = o.o }

// WithObserver 注入类型化观测旁路（缺省无观察者）。回调在调用方
// goroutine 同步执行、须微秒级返回；库内 recover panic。
func WithObserver(o Observer) observerOption { return observerOption{o} }

// observerHub 分发观测事件：nil 观察者零成本直返；回调 recover panic；
// V2 事件经类型断言分发（未实现 V2 的观察者不感知扩展事件）。
type observerHub struct{ obs Observer }

func newObserverHub(o Observer) observerHub { return observerHub{obs: o} }

func (h observerHub) publish(ev PublishEvent) {
	if h.obs == nil {
		return
	}
	safeObserve(func() { h.obs.OnPublish(ev) })
}

func (h observerHub) consume(ev ConsumeEvent) {
	if h.obs == nil {
		return
	}
	safeObserve(func() { h.obs.OnConsume(ev) })
}

func (h observerHub) wake(ev WakeEvent) {
	if h.obs == nil {
		return
	}
	safeObserve(func() { h.obs.OnWake(ev) })
}

func (h observerHub) leaseAbandoned(ev LeaseEvent) {
	if v2, ok := h.obs.(ObserverV2); ok {
		safeObserve(func() { v2.OnLeaseAbandoned(ev) })
	}
}

func (h observerHub) runnerStopped(ev StopEvent) {
	if v2, ok := h.obs.(ObserverV2); ok {
		safeObserve(func() { v2.OnRunnerStopped(ev) })
	}
}

func (h observerHub) claim(ev ClaimEvent) {
	if v4, ok := h.obs.(ObserverV4); ok {
		safeObserve(func() { v4.OnClaim(ev) })
	}
}

func safeObserve(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// publisherMetrics 是发布侧仪表集。构造期从快照 provider 派生 meter 并
// 注册仪表；otel 出错时返回的仪表仍可用（noop），错误不阻断。
type publisherMetrics struct {
	publish         metric.Int64Counter
	publishDuration metric.Float64Histogram
	wake            metric.Int64Counter
}

func newPublisherMetrics(mp metric.MeterProvider, scope string) *publisherMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider() // 兜底：未配置时即 noop provider
	}
	m := mp.Meter(scope)
	pm := &publisherMetrics{}
	pm.publish, _ = m.Int64Counter(scope+".publish",
		metric.WithDescription("事件发布结果计数"), metric.WithUnit("1"))
	pm.publishDuration, _ = m.Float64Histogram(scope+".publish.duration",
		metric.WithDescription("事件发布耗时"), metric.WithUnit("s"))
	pm.wake, _ = m.Int64Counter(scope+".wake",
		metric.WithDescription("就近唤醒信号计数（sent/dropped）"), metric.WithUnit("1"))
	return pm
}

// runnerMetrics 是消费侧仪表集（构建方式同 publisherMetrics）。
type runnerMetrics struct {
	fetch               metric.Int64Counter
	fetchDuration       metric.Float64Histogram
	claim               metric.Int64Counter
	claimDuration       metric.Float64Histogram
	claimGranted        metric.Int64Counter
	claimTakeover       metric.Int64Counter
	claimTerminalFailed metric.Int64Counter
	leaseAbandoned      metric.Int64Counter
	consume             metric.Int64Counter
	handleDuration      metric.Float64Histogram
	heartbeat           metric.Int64Counter
	complete            metric.Int64Counter
	fail                metric.Int64Counter
}

func newRunnerMetrics(mp metric.MeterProvider, scope string) *runnerMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(scope)
	rm := &runnerMetrics{}
	rm.fetch, _ = m.Int64Counter(scope+".fetch",
		metric.WithDescription("广播拉取轮次计数"), metric.WithUnit("1"))
	rm.fetchDuration, _ = m.Float64Histogram(scope+".fetch.duration",
		metric.WithDescription("广播拉取耗时"), metric.WithUnit("s"))
	rm.claim, _ = m.Int64Counter(scope+".claim",
		metric.WithDescription("认领轮次计数"), metric.WithUnit("1"))
	rm.claimDuration, _ = m.Float64Histogram(scope+".claim.duration",
		metric.WithDescription("认领耗时"), metric.WithUnit("s"))
	rm.claimGranted, _ = m.Int64Counter(scope+".claim.granted",
		metric.WithDescription("认领发放租约数"), metric.WithUnit("1"))
	rm.claimTakeover, _ = m.Int64Counter(scope+".claim.takeover",
		metric.WithDescription("认领接管发放数（含接管后直接置 failed 的终态接管；与落库事实一致）"),
		metric.WithUnit("1"))
	rm.claimTerminalFailed, _ = m.Int64Counter(scope+".claim.terminal_failed",
		metric.WithDescription("认领侧达重试上限直接置 failed 数（当前语义必经接管路径，ADR 0006）"),
		metric.WithUnit("1"))
	rm.leaseAbandoned, _ = m.Int64Counter(scope+".lease.abandoned",
		metric.WithDescription("停机放弃租约数（待其他实例接管）"), metric.WithUnit("1"))
	rm.consume, _ = m.Int64Counter(scope+".consume",
		metric.WithDescription("handler 消费结果计数"), metric.WithUnit("1"))
	rm.handleDuration, _ = m.Float64Histogram(scope+".handle.duration",
		metric.WithDescription("handler 处理耗时"), metric.WithUnit("s"))
	rm.heartbeat, _ = m.Int64Counter(scope+".heartbeat",
		metric.WithDescription("租约续租结果计数"), metric.WithUnit("1"))
	rm.complete, _ = m.Int64Counter(scope+".complete",
		metric.WithDescription("租约完成标记结果计数"), metric.WithUnit("1"))
	rm.fail, _ = m.Int64Counter(scope+".fail",
		metric.WithDescription("租约失败标记结果计数"), metric.WithUnit("1"))
	return rm
}

// pollResult 轮询类 driver 操作（fetch/claim）的结果枚举；canceled 口径
// 与既有日志过滤一致（errors.Is(err, context.Canceled)）。
func pollResult(err error) string {
	switch {
	case err == nil:
		return resultOK
	case errors.Is(err, context.Canceled):
		return resultCanceled
	default:
		return resultError
	}
}

// handlerResult handler 消费结果枚举：panic 经 handlerPanicError 判定。
func handlerResult(err error) string {
	switch {
	case err == nil:
		return resultOK
	case isHandlerPanic(err):
		return resultHandlerPanic
	default:
		return resultHandlerError
	}
}

// wakeResult 唤醒信号结果枚举（任一通道丢弃即 dropped）。
func wakeResult(notified, dropped int) string {
	if dropped > 0 {
		return resultDropped
	}
	return resultSent
}

// resultAttrs 单 result 属性的快捷构造。
func resultAttrs(v string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(attrResult, v))
}

// logErrAttrs 日志错误属性（OTel 语义约定：error.type 类型名 + error
// 字符串化，不携带堆栈）。
func logErrAttrs(err error) []any {
	return []any{"error.type", fmt.Sprintf("%T", err), "error", err.Error()}
}

// scopeOrDefault 空值回落缺省 scope。
func scopeOrDefault(s string) string {
	if s != "" {
		return s
	}
	return defaultScope
}
