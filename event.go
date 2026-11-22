package event

import (
	"context"
	"time"
)

// NoPayload 无参事件的载荷占位类型（design.md §4）。
type NoPayload struct{}

// Event[T] 事件声明值。Name 是认领与路由的稳定标识，改名即破坏性变更
// （design.md §4）。值为纯声明，不携带运行时状态；Publish / Subscribe*
// 为强类型糖，把依赖（Publisher / Runner）经参数显式传入。
type Event[T any] struct {
	Name string
}

// Declare 声明强类型事件；无参事件即 Declare[NoPayload](name)。
func Declare[T any](name string) Event[T] {
	return Event[T]{Name: name}
}

// Publish 发布事件（对称糖，等价 Publish(ctx, p, ev, payload, opts...)）。
func (ev Event[T]) Publish(ctx context.Context, p *Publisher,
	payload T, opts ...PublishOption) error {
	return Publish(ctx, p, ev, payload, opts...)
}

// SubscribeBroadcast 注册广播订阅：发完即忘（ADR 0006）。仅接受
// WithConcurrency / WithFetchLimit；认领专属选项返回 ErrNotClaimSubscription。
//
// 注册在 Runner 生命周期内任意时刻可用（Stop 前，ADR 0015）：Start 前
// 注册由 Start 统一装配；启动后注册即时生效，基线锚定于注册调用内
// （注册返回即基线确立，只处理其后发布的事件），锚定失败可修复重试；
// Stop 后注册返回 ErrRunnerStopped。
func (ev Event[T]) SubscribeBroadcast(r *Runner, h Handler[T],
	opts ...SubscribeOption) error {
	return r.subscribeBroadcast(ev.Name, wrapHandler[T](r, h), opts)
}

// SubscribeClaim 注册认领订阅（ADR 0006）。身份取 WithConsumerType，
// 未设置时用 Runner 构造默认身份（WithIdentity），两者皆缺返回 ErrIdentityMissing。
//
// 注册在 Runner 生命周期内任意时刻可用（Stop 前，ADR 0015）：启动后
// 注册即时生效，起点对齐新身份口径——从当前可认领行（保留窗口内，
// 含其他身份已 done/failed 的）开始认领（design.md §5.1）；Stop 后
// 注册返回 ErrRunnerStopped。
func (ev Event[T]) SubscribeClaim(r *Runner, h Handler[T],
	opts ...SubscribeOption) error {
	return r.subscribeClaim(ev.Name, wrapHandler[T](r, h), opts)
}

// Envelope 面向使用方的事件信封（强类型 payload）。
type Envelope[T any] struct {
	// ID 是事件主键、全局唯一（发布侧生成或 WithEventID 显式指定），
	// 幂等去重与追踪主键。跨事件类型复用同一 ID 视为同一事件、第二个
	// payload 被丢弃；业务键须按类型加前缀后才能当 ID 用。
	ID         string
	Type       string // 即 Event.Name
	Payload    T
	Metadata   map[string]string // 透传元数据，如 trace_id（design.md §4）
	OccurredAt time.Time
	// DeliverAt 是发布侧声明的最早投递时刻（WithDeliverAt 设置）；零值
	// 表示立即可投递。仅声明「不早于」语义：到期先后由 Fetch/Claim 按后端
	// 时钟判定，投递本身不保证恰在该时刻发生（最坏等于轮询间隔）。广播
	// 消费下延迟行可能被水位越过而不再投递（ADR 0013 §1 的边界），需要
	// 定时必达用认领消费。
	DeliverAt time.Time
	// CoalesceKey 是合并投递键（WithCoalesceKey 设置，ADR 0014）：同
	// (Type, CoalesceKey) 作用域内最新一次发布取代之前的待消费前驱，
	// 被取代行不再投递。空串表示未设置。与事件 ID 正交：同 ID 幂等
	// Append 语义不变（重复 Append 不触发取代）。
	CoalesceKey string

	// Attempts 是本次投递的尝试序号（含本次）：新认领为「已失败次数 + 1」，
	// 接管认领在先前基础上递增。仅认领消费有意义，广播消费恒为 0。
	Attempts int
	// LastAttempt 表示本次是否为最后一次尝试，由 core 按订阅级 RetryLimit
	// 计算：Attempts >= RetryLimit 时为 true（按 >= 判定，防御多实例间的
	// 订阅配置差异；接管侧在认领时直接置 failed 的路径不投递「最后一次」，
	// handler 不得假设它必然出现）。广播消费恒为 false。
	LastAttempt bool
}

// RawEnvelope 面向 Driver 的事件信封（payload 已编码）。除运行时注入字段
// （Attempts/LastAttempt，见下）外，后端必须原样落库透传（design.md §4）。
type RawEnvelope struct {
	ID         string
	Type       string
	Payload    []byte
	Metadata   map[string]string
	OccurredAt time.Time
	// DeliverAt 是最早投递时刻（零值 = 立即可投递）：后端原样落库（零值
	// 存 NULL），Fetch / Claim 以 q.Now 下推判定 deliver_at <= now，未到期
	// 行在单条操作内被过滤（ADR 0004 过滤发生在读取/认领时）。过去时刻
	// 等价立即可投递，不报错。
	DeliverAt time.Time
	// CoalesceKey 是合并投递键（ADR 0014）：后端原样落库（空串存 NULL）。
	// 同 (Type, CoalesceKey) 作用域内，Append 原子将「无任何认领行的待
	// 消费前驱」置为 superseded，Fetch / Claim 不再返回被取代行（广播
	// 水位直接越过，无新边界）；与事件 ID 正交——重复 Append 同 ID 不
	// 触发取代（幂等语义不变）。空串表示未设置，不参与合并。
	CoalesceKey string
	// Position 是后端定义的全序位置（如 Postgres bigserial），仅用于
	// 广播消费的已读水位条件（ADR 0004）：core 内存按订阅类型维护、
	// 只进不退，Fetch 时原样回传。Append 输入侧忽略此字段。
	Position string

	// Lease.Attempts 是 Claim 侧尝试序号的唯一来源；RawEnvelope.Attempts /
	// LastAttempt 仅由 core 在派发 handler 前写入，Append 输入侧忽略且不落库，
	// Fetch/Claim 返回时必须为零值。Driver 不得依赖它们，也不得把它们作为
	// 存取判据。
	Attempts    int
	LastAttempt bool
}

// Handler 消费处理函数；两种消费方式同一签名（design.md §6）。
// 返回非 nil error 即本次处理失败（广播：记日志跳过；认领：计一次失败重试）。
type Handler[T any] func(ctx context.Context, e Envelope[T]) error

// wrapHandler 把强类型 handler 绑定 Runner 的 codec，变为面向
// RawEnvelope 的内部 handler。
func wrapHandler[T any](r *Runner, h Handler[T]) func(context.Context, RawEnvelope) error {
	return func(ctx context.Context, raw RawEnvelope) error {
		var payload T
		if err := r.codec.Decode(raw.Payload, &payload); err != nil {
			return err
		}
		return h(ctx, Envelope[T]{
			ID:          raw.ID,
			Type:        raw.Type,
			Payload:     payload,
			Metadata:    raw.Metadata,
			OccurredAt:  raw.OccurredAt,
			DeliverAt:   raw.DeliverAt,
			CoalesceKey: raw.CoalesceKey,
			Attempts:    raw.Attempts,
			LastAttempt: raw.LastAttempt,
		})
	}
}
