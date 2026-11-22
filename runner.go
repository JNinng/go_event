package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

	mp    metric.MeterProvider // 选项注入值；nil = 未注入（构造期全局兜底）
	scope string
	obs   Observer

	metrics *runnerMetrics // 构造期从快照 provider 派生
	hub     observerHub

	mu      sync.Mutex
	subs    []*subscription
	started bool
	stopped bool            // Stop 后置位：注册一律拒绝（Runner 一次性，ADR 0015）
	runCtx  context.Context // poller 生命周期 context：Start 建立后供动态注册复用
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
	r.metrics = newRunnerMetrics(r.mp, scopeOrDefault(r.scope))
	r.hub = newObserverHub(r.obs)
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

// WithFetchLimit 每轮按类型限额取数（ADR 0004）。认领侧实际批大小另受
// 空闲并发槽位封顶（usage.md §6.6）。
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

// modeName 订阅模式的观测属性取值（attrMode）。
func modeName(m subMode) string {
	if m == modeClaim {
		return modeNameClaim
	}
	return modeNameBroadcast
}

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
	claimCh   chan struct{} // 内部补认领信号：槽位释放后立即再认领一轮
	watermark string        // 广播消费已读水位：core 内存维护，只进不退（ADR 0004）
}

func (r *Runner) subscribeBroadcast(typ string,
	h func(context.Context, RawEnvelope) error, opts []SubscribeOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.registrationGuard(); err != nil {
		return err
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
	return r.register(s)
}

func (r *Runner) subscribeClaim(typ string,
	h func(context.Context, RawEnvelope) error, opts []SubscribeOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.registrationGuard(); err != nil {
		return err
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
		// 即会被接管）一律取缺省 lease/3；钳制契约见 docs/api.md §2.5。
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
	return r.register(s)
}

// registrationGuard 注册闸门（两订阅入口共用，调用方持有 r.mu）：Stop 后
// ——含 Start ctx 已取消、poller 已退出的窗口——注册一律拒绝；未启动与
// 运行中放行（运行中经 register 即时生效，ADR 0015）。
func (r *Runner) registrationGuard() error {
	if r.stopped || (r.started && r.runCtx.Err() != nil) {
		return ErrRunnerStopped
	}
	return nil
}

// register 把订阅写入注册表（调用方持有 r.mu）：未启动时仅入表，由 Start
// 统一装配并锚定基线；启动后即时装配生效（ADR 0015）——广播订阅在注册
// 调用内同步锚定基线（HeadPosition），注册返回即基线确立；锚定失败则
// 订阅不入表，既有订阅不受影响，修复后可重试注册。
func (r *Runner) register(s *subscription) error {
	if !r.started {
		r.subs = append(r.subs, s)
		return nil
	}
	if s.mode == modeBroadcast {
		pos, err := r.drv.HeadPosition(r.runCtx, s.typ)
		if err != nil {
			r.metrics.subscribe.Add(r.runCtx, 1, metric.WithAttributes(
				attribute.String(attrEvent, s.typ),
				attribute.String(attrMode, modeName(s.mode)),
				attribute.String(attrResult, resultError)))
			return fmt.Errorf("event: broadcast baseline for %q: %w", s.typ, err)
		}
		s.watermark = pos
	}
	r.startSubscription(r.runCtx, s)
	r.subs = append(r.subs, s)
	r.logger.Info("event: subscription registered",
		"type", s.typ, "mode", modeName(s.mode), "subscriptions", len(r.subs))
	r.metrics.subscribe.Add(r.runCtx, 1, metric.WithAttributes(
		attribute.String(attrEvent, s.typ),
		attribute.String(attrMode, modeName(s.mode)),
		attribute.String(attrResult, resultOK)))
	return nil
}

// startSubscription 装配并启动单个订阅的 poller（Start 与动态注册共用，
// 调用方持有 r.mu）；runCtx 是该订阅 poller 的生命周期 context。
func (r *Runner) startSubscription(runCtx context.Context, s *subscription) {
	s.sem = make(chan struct{}, s.concurrency)
	if s.mode == modeClaim {
		// 容量 1、非阻塞发送：handler 结束释放槽位后立即补认领，
		// 积压消化不依赖轮询间隔；多次释放合并为一次信号。
		s.claimCh = make(chan struct{}, 1)
	}
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

// Start 启动全部已注册订阅的 poller（返回后后台运行）。重复 Start 为无
// 操作。启动后可继续经 SubscribeBroadcast / SubscribeClaim 动态注册订阅，
// 即时生效（ADR 0015）。
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
	r.runCtx = runCtx
	r.cancel = cancel
	r.started = true
	for _, s := range r.subs {
		r.startSubscription(runCtx, s)
	}
	r.logger.Info("event: runner started", "subscriptions", len(r.subs))
	return nil
}

// Stop 优雅停机：停止认领/拉取，等待在途 handler 跑完（design.md §9.1）。
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	wasStarted := false
	if r.cancel != nil {
		wasStarted = true
		// Runner 一次性：Stop 后注册一律拒绝（ADR 0015）。未启动的 Stop
		// 仍是无操作，Runner 保持可用。
		r.stopped = true
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
		if wasStarted {
			r.logger.Info("event: runner stopped")
			r.hub.runnerStopped(StopEvent{})
		}
		return nil
	case <-ctx.Done():
		if wasStarted {
			r.logger.Warn("event: runner stop timed out, in-flight handlers continuing",
				logErrAttrs(ctx.Err())...)
			r.hub.runnerStopped(StopEvent{TimedOut: true})
		}
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
	start := time.Now()
	batch, err := r.drv.Fetch(ctx, FetchQuery{
		Type: s.typ, After: s.watermark, Limit: s.fetchLimit,
		Now: time.Now(), // 延迟事件到期判据时钟下推（ADR 0013）
	})
	res := pollResult(err)
	attrs := resultAttrs(res)
	r.metrics.fetch.Add(ctx, 1, attrs)
	r.metrics.fetchDuration.Record(ctx, time.Since(start).Seconds(), attrs)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("event: broadcast fetch failed",
				append([]any{"type", s.typ}, logErrAttrs(err))...)
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
	// 广播不消费尝试次数：无视 Fetch/Driver 可能带回的任何值，强制零值，
	// 使「广播恒为 0/false」不依赖后端自律。
	raw.Attempts = 0
	raw.LastAttempt = false
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
		hctx := context.WithoutCancel(ctx)
		start := time.Now()
		err := callHandler(s.handler, hctx, raw)
		res := handlerResult(err)
		hAttrs := metric.WithAttributes(
			attribute.String(attrMode, modeNameBroadcast),
			attribute.String(attrEvent, s.typ),
			attribute.String(attrResult, res))
		r.metrics.consume.Add(hctx, 1, hAttrs)
		r.metrics.handleDuration.Record(hctx, time.Since(start).Seconds(), hAttrs)
		r.hub.consume(ConsumeEvent{Mode: modeNameBroadcast, Event: s.typ, Result: res, Err: err})
		if err != nil {
			// 广播消费失败即跳过：不重试、不阻塞、不设死信（ADR 0006）。
			r.logger.Warn("event: broadcast handler failed, skipped",
				append([]any{"type", s.typ, "event_id", raw.ID}, logErrAttrs(err))...)
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
		case <-s.claimCh:
			// 有槽位释放：立即补认领，不等轮询间隔。
		}
		r.pollClaim(ctx, s)
		timer.Reset(r.pollInterval)
	}
}

// pollClaim 认领一轮：有效批大小取 min(FetchLimit, 空闲并发槽位)，
// 无空闲槽位时不认领。由此 Claim 返回的每条租约都能立即派发、立即启动
// 心跳，不存在「已认领但排队等槽位、期间无人续租」的窗口；FetchLimit
// 对认领是批次上限，不要求 ≤ Concurrency（缺省 100 > 1 合法）。
func (r *Runner) pollClaim(ctx context.Context, s *subscription) {
	free := cap(s.sem) - len(s.sem)
	if free <= 0 {
		return
	}
	limit := s.fetchLimit
	if free < limit {
		limit = free
	}
	start := time.Now()
	result, err := r.drv.Claim(ctx, ClaimQuery{
		Type:         s.typ,
		ConsumerType: s.consumerType,
		Lease:        s.lease,
		Limit:        limit,
		RetryLimit:   s.retryLimit,
		Now:          time.Now(),
	})
	res := pollResult(err)
	attrs := resultAttrs(res)
	r.metrics.claim.Add(ctx, 1, attrs)
	r.metrics.claimDuration.Record(ctx, time.Since(start).Seconds(), attrs)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("event: claim failed",
				append([]any{"type", s.typ, "consumer", s.consumerType}, logErrAttrs(err))...)
		}
		return
	}
	// 接管与终态计数随 ClaimResult 返回，与落库事实一致（design.md §9.2）；
	// 各自条件打点（仅计数非零），不产生空轮次的零值数据点。接管/终态
	// 独立计量，补齐 ADR 0008 已知口径限制（Attempts 混合 Fail 重试与
	// 过期接管，不因本计量改变）。
	if result.Takeovers > 0 || result.TerminalFailed > 0 {
		cAttrs := metric.WithAttributes(
			attribute.String(attrEvent, s.typ),
			attribute.String(attrIdentity, s.consumerType))
		if result.Takeovers > 0 {
			r.metrics.claimTakeover.Add(ctx, int64(result.Takeovers), cAttrs)
		}
		if result.TerminalFailed > 0 {
			r.metrics.claimTerminalFailed.Add(ctx, int64(result.TerminalFailed), cAttrs)
		}
	}
	for _, lease := range result.Leases {
		gAttrs := metric.WithAttributes(
			attribute.String(attrEvent, s.typ),
			attribute.String(attrIdentity, s.consumerType))
		r.metrics.claimGranted.Add(ctx, 1, gAttrs)
		r.dispatchClaim(ctx, s, lease)
	}
	r.hub.claim(ClaimEvent{
		Event: s.typ, Identity: s.consumerType,
		Leases: len(result.Leases), Takeovers: result.Takeovers,
		TerminalFailed: result.TerminalFailed,
	})
}

func (r *Runner) dispatchClaim(ctx context.Context, s *subscription, lease Lease) {
	// 正常契约下（Driver 至多返回 Limit 条，且 Limit ≤ 空闲槽位）这里必然
	// 立即取得槽位。后端违约返回超额批次时，多出的租约在此排队等槽位，
	// 属违约后端的兜底；只有停机（ctx.Done）才放弃派发，租约到期后由其
	// 他实例接管（at-least-once 不破坏）。
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		// 停机路径：放弃租约待接管（细节见函数头注释）。
		r.metrics.leaseAbandoned.Add(ctx, 1,
			metric.WithAttributes(attribute.String(attrEvent, s.typ)))
		r.hub.leaseAbandoned(LeaseEvent{Event: s.typ})
		r.logger.Info("event: stopping, lease released for takeover",
			"type", s.typ, "event_id", lease.Envelope.ID)
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() {
			<-s.sem
			// 槽位释放后非阻塞补一次认领信号：容量 1，堆积合并；Stop
			// 路径上 runClaim 已退出，信号最多留在缓冲里，不阻塞。
			r.signalClaim(s)
		}()
		r.processClaim(ctx, s, lease)
	}()
}

// signalClaim 向订阅投递一次补认领信号（非阻塞、可合并）。
func (r *Runner) signalClaim(s *subscription) {
	select {
	case s.claimCh <- struct{}{}:
	default:
	}
}

// processClaim 处理单个租约：心跳续租 + Complete/Fail（ADR 0006）。
func (r *Runner) processClaim(ctx context.Context, s *subscription, lease Lease) {
	hbDone := make(chan struct{})
	r.wg.Add(1)
	go r.heartbeatLoop(ctx, s, lease, hbDone)

	// 尝试次数是 core 运行时注入字段：由租约的尝试序号与订阅级
	// RetryLimit 计算，经 RawEnvelope 原样映射到 Envelope；Driver 不感知。
	lease.Envelope.Attempts = lease.Attempts
	lease.Envelope.LastAttempt = lease.Attempts >= s.retryLimit

	// handler 用不取消的 ctx：停机等在途跑完（design.md §9.1），不因
	// 停机取消把在途 handler 记为失败、消耗重试计数；Stop(ctx) 的
	// ctx 超时是调用方兜底。Complete/Fail 同理用不取消的 ctx：
	// 在途 handler 的终态操作仍需生效（CAS 不命中时后端静默返回 nil）。
	start := time.Now()
	err := callHandler(s.handler, context.WithoutCancel(ctx), lease.Envelope)
	close(hbDone)

	res := handlerResult(err)
	tctx := context.WithoutCancel(ctx)
	hAttrs := metric.WithAttributes(
		attribute.String(attrMode, modeNameClaim),
		attribute.String(attrEvent, s.typ),
		attribute.String(attrResult, res))
	r.metrics.handleDuration.Record(tctx, time.Since(start).Seconds(), hAttrs)
	r.metrics.consume.Add(tctx, 1, hAttrs)
	r.hub.consume(ConsumeEvent{
		Mode: modeNameClaim, Event: s.typ, Result: res,
		Attempts: lease.Attempts, Err: err,
	})
	if err != nil {
		r.logger.Warn("event: claim handler failed",
			append([]any{"type", s.typ, "consumer", s.consumerType, "event_id",
				lease.Envelope.ID, "attempts", lease.Attempts}, logErrAttrs(err))...)
		if ferr := r.drv.Fail(tctx, FailRequest{
			EventID:      lease.Envelope.ID,
			ConsumerType: lease.ConsumerType,
			LeaseToken:   lease.LeaseToken,
			// handler 错误文本下推为终态摘要：后端清洗后仅在 failed 终态
			// 落库（不落堆栈——堆栈由 handler 侧观测承担）。
			Error:      err.Error(),
			RetryLimit: s.retryLimit,
			Backoff:    s.backoff,
			Now:        time.Now(),
		}); ferr != nil {
			r.metrics.fail.Add(tctx, 1, resultAttrs(resultError))
			r.logger.Error("event: fail recording failed",
				append([]any{"event_id", lease.Envelope.ID}, logErrAttrs(ferr))...)
			return
		}
		r.metrics.fail.Add(tctx, 1, resultAttrs(resultOK))
		return
	}
	if cerr := r.drv.Complete(tctx, CompleteRequest{
		EventID:      lease.Envelope.ID,
		ConsumerType: lease.ConsumerType,
		LeaseToken:   lease.LeaseToken,
		Now:          time.Now(),
	}); cerr != nil {
		r.metrics.complete.Add(tctx, 1, resultAttrs(resultError))
		r.logger.Error("event: complete failed",
			append([]any{"event_id", lease.Envelope.ID}, logErrAttrs(cerr))...)
		return
	}
	r.metrics.complete.Add(tctx, 1, resultAttrs(resultOK))
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
				r.metrics.heartbeat.Add(hctx, 1, resultAttrs(resultError))
				r.logger.Warn("event: heartbeat failed, retry next tick",
					append([]any{"event_id", lease.Envelope.ID}, logErrAttrs(err))...)
			} else {
				r.metrics.heartbeat.Add(hctx, 1, resultAttrs(resultOK))
			}
		}
	}
}

// handlerPanicError 是 handler panic 的包装错误（消息文本稳定），
// 供观测分类以 errors.As 判别 handler_panic。
type handlerPanicError struct{ value any }

func (e handlerPanicError) Error() string {
	return fmt.Sprintf("event: handler panic: %v", e.value)
}

func isHandlerPanic(err error) bool {
	var perr handlerPanicError
	return errors.As(err, &perr)
}

// callHandler 执行 handler，panic 转为 error（认领计一次失败重试，
// 广播记日志跳过）。
func callHandler(h func(context.Context, RawEnvelope) error,
	ctx context.Context, raw RawEnvelope) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = handlerPanicError{value: p}
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
