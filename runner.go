package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Runner 消费编排器（design.md §4）：poll 循环、心跳、重试上限、就近唤醒、
// 并发度控制、优雅停机全部在此完成；Driver 只做单次原子操作。
type Runner struct {
	drv          Driver
	identity     string
	codec        Codec
	pollInterval time.Duration
	wake         *LocalWake
	logger       *slog.Logger

	mu      sync.Mutex
	subs    []*subscription
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

const (
	defaultLease         = 30 * time.Second
	defaultMaxProcessing = 10 * time.Minute
	defaultRetryLimit    = 5
	defaultConcurrency   = 1
	defaultFetchLimit    = 100
	defaultPollInterval  = 5 * time.Second
)

// RunnerOption Runner 构造选项（接口形态，见 PublisherOption 注释）。
type RunnerOption interface {
	applyRunner(*Runner)
}

// NewRunner 构造消费编排器。同一 Runner 可混合承载广播与认领订阅；
// 消费方式与订阅配置在 Event[T].SubscribeBroadcast / SubscribeClaim
// 处声明。
func NewRunner(d Driver, opts ...RunnerOption) *Runner {
	r := &Runner{
		drv:          d,
		codec:        NewJSONCodec(),
		pollInterval: defaultPollInterval,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt.applyRunner(r)
	}
	return r
}

type identityOption struct{ s string }

func (o identityOption) applyRunner(r *Runner) { r.identity = o.s }

// WithIdentity 认领订阅的默认消费者身份（design.md §5.1，订阅级可覆盖）。
func WithIdentity(identity string) RunnerOption { return identityOption{identity} }

type pollOption struct{ d time.Duration }

func (o pollOption) applyRunner(r *Runner) { r.pollInterval = o.d }

// WithPollInterval 设置轮询间隔。
func WithPollInterval(d time.Duration) RunnerOption { return pollOption{d} }

type loggerOption struct{ l *slog.Logger }

func (o loggerOption) applyRunner(r *Runner) { r.logger = o.l }

// WithLogger 注入 slog Logger（缺省 slog.Default()）。
func WithLogger(l *slog.Logger) RunnerOption { return loggerOption{l} }

// SubscribeOption 广播与认领均可用的订阅选项。
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	concurrency int
	fetchLimit  int
	claimOnly   bool // 认领专属选项标记：广播订阅传入即 ErrNotClaimSubscription

	consumerType  string
	lease         time.Duration
	heartbeat     time.Duration
	maxProcessing time.Duration
	retryLimit    int
	backoff       Backoff
}

// WithFetchLimit 每轮按类型限额取数（ADR 0004）。
func WithFetchLimit(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.fetchLimit = n }
}

// WithConcurrency handler 并发度。
func WithConcurrency(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.concurrency = n }
}

// WithConsumerType 覆盖 Runner 默认身份（仅认领订阅可用）。
func WithConsumerType(identity string) SubscribeOption {
	return func(c *subscribeConfig) { c.consumerType, c.claimOnly = identity, true }
}

// WithLease 租约时长（缺省 30s，仅认领订阅可用）。
func WithLease(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) { c.lease, c.claimOnly = d, true }
}

// WithHeartbeat 心跳间隔（缺省 lease/3，仅认领订阅可用）。
func WithHeartbeat(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) { c.heartbeat, c.claimOnly = d, true }
}

// WithMaxProcessing 最大处理时长，超过停止续租（缺省 10min，ADR 0006，
// 仅认领订阅可用）。
func WithMaxProcessing(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) { c.maxProcessing, c.claimOnly = d, true }
}

// WithRetryLimit 重试上限（缺省 5，仅认领订阅可用）。
func WithRetryLimit(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.retryLimit, c.claimOnly = n, true }
}

// WithBackoff 递增退避（缺省 ExponentialBackoff(1s, 1h)，仅认领订阅可用）。
func WithBackoff(b Backoff) SubscribeOption {
	return func(c *subscribeConfig) { c.backoff, c.claimOnly = b, true }
}

type subMode int

const (
	modeBroadcast subMode = iota
	modeClaim
)

type subscription struct {
	mode    subMode
	typ     string
	handler func(context.Context, RawEnvelope) error

	concurrency int
	fetchLimit  int

	// 仅认领订阅
	consumerType  string
	lease         time.Duration
	heartbeat     time.Duration
	maxProcessing time.Duration
	retryLimit    int
	backoff       Backoff

	// 运行时
	sem       chan struct{}
	wakeCh    chan struct{}
	watermark string // 广播消费已读水位：core 内存维护，只进不退（ADR 0004）
}

func (r *Runner) subscribeBroadcast(typ string,
	h func(context.Context, RawEnvelope) error, opts []SubscribeOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return ErrRunnerStarted
	}
	cfg := subscribeConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.claimOnly {
		return ErrNotClaimSubscription
	}
	s := &subscription{
		mode:        modeBroadcast,
		typ:         typ,
		handler:     h,
		concurrency: maxOr(cfg.concurrency, defaultConcurrency),
		fetchLimit:  maxOr(cfg.fetchLimit, defaultFetchLimit),
	}
	r.subs = append(r.subs, s)
	return nil
}

func (r *Runner) subscribeClaim(typ string,
	h func(context.Context, RawEnvelope) error, opts []SubscribeOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return ErrRunnerStarted
	}
	cfg := subscribeConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	identity := cfg.consumerType
	if identity == "" {
		identity = r.identity
	}
	if identity == "" {
		return ErrIdentityMissing
	}
	lease := cfg.lease
	if lease <= 0 {
		lease = defaultLease
	}
	heartbeat := cfg.heartbeat
	if heartbeat <= 0 || heartbeat >= lease {
		// 缺省或非法（≥ 租约的首个心跳落在租约过期之后，handler 仍在跑
		// 即会被接管）一律取缺省 lease/3；钳制行为写进 docs/api.md §2.5。
		heartbeat = lease / 3
	}
	maxProcessing := cfg.maxProcessing
	if maxProcessing <= 0 {
		maxProcessing = defaultMaxProcessing
	}
	backoff := cfg.backoff
	if backoff == nil {
		backoff = ExponentialBackoff(time.Second, time.Hour)
	}
	s := &subscription{
		mode:          modeClaim,
		typ:           typ,
		handler:       h,
		concurrency:   maxOr(cfg.concurrency, defaultConcurrency),
		fetchLimit:    maxOr(cfg.fetchLimit, defaultFetchLimit),
		consumerType:  identity,
		lease:         lease,
		heartbeat:     heartbeat,
		maxProcessing: maxProcessing,
		retryLimit:    maxOr(cfg.retryLimit, defaultRetryLimit),
		backoff:       backoff,
	}
	r.subs = append(r.subs, s)
	return nil
}

// Start 启动全部订阅的 poller（返回后后台运行）。重复 Start 为无操作。
//
// 广播基线在 Start 内同步确立（ADR 0006）：对每个广播订阅调用
// Driver.HeadPosition，水位锚定到该类型当前已提交的最大位置——Start
// 返回即基线确立，此后发布的事件必然可投递。任一锚点查询失败则整体
// 不启动并返回错误，可修复后重试 Start。
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	for _, s := range r.subs {
		if s.mode != modeBroadcast {
			continue
		}
		pos, err := r.drv.HeadPosition(ctx, s.typ)
		if err != nil {
			return fmt.Errorf("event: broadcast baseline for %q: %w", s.typ, err)
		}
		s.watermark = pos
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.started = true
	for _, s := range r.subs {
		s.sem = make(chan struct{}, s.concurrency)
		if r.wake != nil {
			s.wakeCh = r.wake.subscribe(s.typ)
		}
		r.wg.Add(1)
		if s.mode == modeBroadcast {
			go r.runBroadcast(runCtx, s)
		} else {
			go r.runClaim(runCtx, s)
		}
	}
	return nil
}

// Stop 优雅停机：停止认领/拉取，等待在途 handler 跑完（design.md §9.1）。
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run 便捷封装：Start 后阻塞至 ctx 取消，再 Stop。
func (r *Runner) Run(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return r.Stop(context.WithoutCancel(ctx))
}

// runBroadcast 广播消费 poll 循环：发完即忘（ADR 0006）。水位已由
// Start 锚定到该类型的头部位置，循环只读取其后发布的事件。
func (r *Runner) runBroadcast(ctx context.Context, s *subscription) {
	defer r.wg.Done()
	if s.wakeCh != nil {
		defer r.wake.unsubscribe(s.typ, s.wakeCh)
	}
	timer := time.NewTimer(r.pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wakeCh:
		}
		r.pollBroadcast(ctx, s)
		timer.Reset(r.pollInterval)
	}
}

func (r *Runner) pollBroadcast(ctx context.Context, s *subscription) {
	batch, err := r.drv.Fetch(ctx, FetchQuery{Type: s.typ, After: s.watermark, Limit: s.fetchLimit})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("event: broadcast fetch failed", "type", s.typ, "err", err)
		}
		return
	}
	for i, raw := range batch {
		if i == len(batch)-1 {
			// Fetch 保证按 Position 升序且严格大于 After，故批次末位即
			// 新水位；内存水位由此只进不退（单调守卫由后端排序保证）。
			s.watermark = raw.Position
		}
		r.dispatchBroadcast(ctx, s, raw)
	}
}

func (r *Runner) dispatchBroadcast(ctx context.Context, s *subscription, raw RawEnvelope) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return // 停机中不再分发；发完即忘，不补投
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-s.sem }()
		// handler 用不取消的 ctx：优雅停机等在途跑完（design.md §9.1），
		// 不因停机取消而把在途事件记为失败（广播跳过/认领计次）。
		if err := callHandler(s.handler, context.WithoutCancel(ctx), raw); err != nil {
			// 广播消费失败即跳过：不重试、不阻塞、不设死信（ADR 0006）。
			r.logger.Warn("event: broadcast handler failed, skipped",
				"type", s.typ, "event_id", raw.ID, "err", err)
		}
	}()
}

// runClaim 认领消费 poll 循环：按类型轮询 Claim（ADR 0004）。
func (r *Runner) runClaim(ctx context.Context, s *subscription) {
	defer r.wg.Done()
	if s.wakeCh != nil {
		defer r.wake.unsubscribe(s.typ, s.wakeCh)
	}
	r.pollClaim(ctx, s) // 启动即首轮认领
	timer := time.NewTimer(r.pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wakeCh:
		}
		r.pollClaim(ctx, s)
		timer.Reset(r.pollInterval)
	}
}

func (r *Runner) pollClaim(ctx context.Context, s *subscription) {
	leases, err := r.drv.Claim(ctx, ClaimQuery{
		Type:         s.typ,
		ConsumerType: s.consumerType,
		Lease:        s.lease,
		Limit:        s.fetchLimit,
		RetryLimit:   s.retryLimit,
		Now:          time.Now(),
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("event: claim failed", "type", s.typ,
				"consumer", s.consumerType, "err", err)
		}
		return
	}
	for _, lease := range leases {
		r.dispatchClaim(ctx, s, lease)
	}
}

func (r *Runner) dispatchClaim(ctx context.Context, s *subscription, lease Lease) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		// 停机中不处理：租约到期由其他实例接管（at-least-once 不破坏）。
		r.logger.Info("event: stopping, lease released for takeover",
			"type", s.typ, "event_id", lease.Envelope.ID)
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-s.sem }()
		r.processClaim(ctx, s, lease)
	}()
}

// processClaim 处理单个租约：心跳续租 + Complete/Fail（ADR 0006）。
func (r *Runner) processClaim(ctx context.Context, s *subscription, lease Lease) {
	hbDone := make(chan struct{})
	r.wg.Add(1)
	go r.heartbeatLoop(ctx, s, lease, hbDone)

	// handler 用不取消的 ctx：停机等在途跑完（design.md §9.1），不因
	// 停机取消把在途 handler 记为失败、消耗重试计数；Stop(ctx) 的
	// ctx 超时是调用方兜底。Complete/Fail 同理用不取消的 ctx：
	// 在途 handler 的终态操作仍需生效（CAS 不命中时后端静默返回 nil）。
	err := callHandler(s.handler, context.WithoutCancel(ctx), lease.Envelope)
	close(hbDone)

	tctx := context.WithoutCancel(ctx)
	if err != nil {
		r.logger.Warn("event: claim handler failed",
			"type", s.typ, "consumer", s.consumerType, "event_id",
			lease.Envelope.ID, "attempts", lease.Attempts, "err", err)
		if ferr := r.drv.Fail(tctx, FailRequest{
			EventID:      lease.Envelope.ID,
			ConsumerType: lease.ConsumerType,
			LeaseToken:   lease.LeaseToken,
			RetryLimit:   s.retryLimit,
			Backoff:      s.backoff,
			Now:          time.Now(),
		}); ferr != nil {
			r.logger.Error("event: fail recording failed", "event_id", lease.Envelope.ID, "err", ferr)
		}
		return
	}
	if cerr := r.drv.Complete(tctx, CompleteRequest{
		EventID:      lease.Envelope.ID,
		ConsumerType: lease.ConsumerType,
		LeaseToken:   lease.LeaseToken,
		Now:          time.Now(),
	}); cerr != nil {
		r.logger.Error("event: complete failed", "event_id", lease.Envelope.ID, "err", cerr)
	}
}

// heartbeatLoop 在订阅级最大处理时长内自动心跳续租；超过最大处理时长
// 停止续租，租约到期由其他实例接管——挂死 handler 不被无限续保（ADR 0006）。
func (r *Runner) heartbeatLoop(ctx context.Context, s *subscription, lease Lease, hbDone <-chan struct{}) {
	defer r.wg.Done()
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	deadline := time.Now().Add(s.maxProcessing)
	hctx := context.WithoutCancel(ctx)
	for {
		select {
		case <-hbDone:
			return
		case <-ticker.C:
			if !time.Now().Before(deadline) {
				r.logger.Warn("event: max processing time exceeded, stop renewing lease",
					"type", s.typ, "event_id", lease.Envelope.ID)
				return
			}
			err := r.drv.Heartbeat(hctx, HeartbeatRequest{
				EventID:      lease.Envelope.ID,
				ConsumerType: lease.ConsumerType,
				LeaseToken:   lease.LeaseToken,
				Extend:       s.lease,
				Now:          time.Now(),
			})
			if err != nil {
				// 瞬时错误（DB 抖动）不停止续租：记日志、下个心跳间隔重试；
				// 续租的停止只由最大处理时长决定（ADR 0006）。租约真丢失
				// （已被接管）时 Heartbeat CAS 不命中静默返回 nil，不会走到这里。
				r.logger.Warn("event: heartbeat failed, retry next tick",
					"event_id", lease.Envelope.ID, "err", err)
			}
		}
	}
}

// callHandler 执行 handler，panic 转为 error（认领计一次失败重试，
// 广播记日志跳过）。
func callHandler(h func(context.Context, RawEnvelope) error,
	ctx context.Context, raw RawEnvelope) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("event: handler panic: %v", p)
		}
	}()
	return h(ctx, raw)
}

func maxOr(n, d int) int {
	if n > 0 {
		return n
	}
	return d
}
