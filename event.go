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
// WithConcurrency / WithFetchLimit；传入认领专属选项返回
// ErrNotClaimSubscription。
func (ev Event[T]) SubscribeBroadcast(r *Runner, h Handler[T],
	opts ...SubscribeOption) error {
	return r.subscribeBroadcast(ev.Name, wrapHandler[T](r, h), opts)
}

// SubscribeClaim 注册认领订阅（ADR 0006）。身份取 WithConsumerType，
// 未设置时用 Runner 构造默认身份（WithIdentity）；两者皆缺返回
// ErrIdentityMissing。
func (ev Event[T]) SubscribeClaim(r *Runner, h Handler[T],
	opts ...SubscribeOption) error {
	return r.subscribeClaim(ev.Name, wrapHandler[T](r, h), opts)
}

// Envelope 面向使用方的事件信封（强类型 payload）。
type Envelope[T any] struct {
	ID         string // 发布侧生成的唯一 ID；幂等去重与追踪主键
	Type       string // 即 Event.Name
	Payload    T
	Metadata   map[string]string // 透传元数据，如 trace_id（design.md §4）
	OccurredAt time.Time
}

// RawEnvelope 面向 Driver 的事件信封（payload 已编码）。后端必须原样
// 落库透传（design.md §4）。
type RawEnvelope struct {
	ID         string
	Type       string
	Payload    []byte
	Metadata   map[string]string
	OccurredAt time.Time
	// Position 是后端定义的全序位置（如 Postgres bigserial），仅用于
	// 广播消费的已读水位条件（ADR 0004）：core 内存按订阅类型维护、
	// 只进不退，Fetch 时原样回传。Append 输入侧忽略此字段。
	Position string
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
			ID:         raw.ID,
			Type:       raw.Type,
			Payload:    payload,
			Metadata:   raw.Metadata,
			OccurredAt: raw.OccurredAt,
		})
	}
}
