package event

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// IDGenerator 事件 ID 生成器；发布侧注入（缺省内置实现）。
type IDGenerator func() string

// Publisher 发布实现。依赖注入：Driver、ID 生成器、codec、就近唤醒总线，
// 全部经 NewPublisher 构造级注入（design.md §4）。
type Publisher struct {
	drv   Driver
	idgen IDGenerator
	codec Codec
	wake  *LocalWake
}

// PublisherOption 生产者构造选项。与 RunnerOption 同为接口形态：
// 同时实现两侧 apply 方法的选项（如 WithLocalWake 与 codec 选项）可注入
// 任一构造器；其余选项传错构造器会编译报错（docs/api.md §2.3/§2.5
// 同名选项）。
type PublisherOption interface {
	applyPublisher(*Publisher)
}

// NewPublisher 构造生产者。
func NewPublisher(d Driver, opts ...PublisherOption) *Publisher {
	p := &Publisher{
		drv:   d,
		idgen: defaultIDGenerator,
		codec: NewJSONCodec(),
	}
	for _, opt := range opts {
		opt.applyPublisher(p)
	}
	return p
}

type codecOption struct{ c Codec }

func (o codecOption) applyPublisher(p *Publisher) { p.codec = o.c }
func (o codecOption) applyRunner(r *Runner)       { r.codec = o.c }

// WithPublisherCodec 注入 payload codec（缺省 JSON）。
func WithPublisherCodec(c Codec) PublisherOption { return codecOption{c} }

// WithRunnerCodec 注入消费侧 payload codec（缺省 JSON）；与
// WithPublisherCodec 共享同一实现（docs/api.md §2.5）。
func WithRunnerCodec(c Codec) RunnerOption { return codecOption{c} }

type idGenOption struct{ g IDGenerator }

func (o idGenOption) applyPublisher(p *Publisher) { p.idgen = o.g }

// WithIDGenerator 注入事件 ID 生成器。
func WithIDGenerator(g IDGenerator) PublisherOption { return idGenOption{g} }

type wakeOption struct{ w *LocalWake }

func (o wakeOption) applyPublisher(p *Publisher) { p.wake = o.w }
func (o wakeOption) applyRunner(r *Runner)       { r.wake = o.w }

// WithLocalWake 注入就近唤醒总线（ADR 0003）：单一选项同时可传给
// NewPublisher（发布成功后发唤醒信号）与 NewRunner（唤醒路由入口）。
func WithLocalWake(w *LocalWake) wakeOption { return wakeOption{w} }

// PublishOption 发布选项。
type PublishOption func(*publishConfig)

type publishConfig struct {
	metadata map[string]string
}

// WithMetadata 合并进缺省元数据（如 trace_id，design.md §4）。
func WithMetadata(meta map[string]string) PublishOption {
	return func(c *publishConfig) {
		if c.metadata == nil {
			c.metadata = make(map[string]string, len(meta))
		}
		for k, v := range meta {
			c.metadata[k] = v
		}
	}
}

// Publish 发布实现：ID 与 OccurredAt 缺省自动填充（design.md §6）。
// 事件侧糖 Event[T].Publish 委托到此。
//
// Go 方法不支持类型参数，故为包级泛型函数而非 Publisher 方法。
func Publish[T any](ctx context.Context, p *Publisher, ev Event[T],
	payload T, opts ...PublishOption) error {
	var cfg publishConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	encoded, err := p.codec.Encode(payload)
	if err != nil {
		return err
	}
	raw := RawEnvelope{
		ID:         p.idgen(),
		Type:       ev.Name,
		Payload:    encoded,
		Metadata:   cfg.metadata,
		OccurredAt: time.Now().UTC(),
	}
	if err := p.drv.Append(ctx, raw); err != nil {
		return err
	}
	if p.wake != nil {
		p.wake.notify(ev.Name) // 仅信号，可丢弃；poll 兜底（ADR 0003）
	}
	return nil
}

// PublishNoPayload 无参事件糖，等价 Publish(ctx, p, ev, NoPayload{})。
func (p *Publisher) PublishNoPayload(ctx context.Context, ev Event[NoPayload],
	opts ...PublishOption) error {
	return Publish(ctx, p, ev, NoPayload{}, opts...)
}

// defaultIDGenerator 内置 ID 生成：crypto/rand 16 字节 hex。
func defaultIDGenerator() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极罕见；退化为时间戳拼接保证可用性。
		return hex.EncodeToString([]byte(time.Now().String()))[:32]
	}
	return hex.EncodeToString(b[:])
}
