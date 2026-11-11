package eventtest

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// 广播基线在 Start 内同步确立：启动前发布的事件不投递，Start 返回后
// 发布的事件必然投递（ADR 0006：水位锚定到 HeadPosition 快照）。
func testBroadcastOnlyAfterStart(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("bc-start-" + t.Name())
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// 启动前发布：基线越过，不补投。
	mustPublish(t, ctx, pub, ev, suitePayload{"pre1"})
	mustPublish(t, ctx, pub, ev, suitePayload{"pre2"})

	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	// Start 已返回即基线确立，无需等待：之后发布必然可投递。
	mustPublish(t, ctx, pub, ev, suitePayload{"post1"})
	mustPublish(t, ctx, pub, ev, suitePayload{"post2"})
	waitFor(t, 5*time.Second, "post-start events delivered", func() bool {
		return count.Load() == 2
	})
	time.Sleep(100 * time.Millisecond) // 负向窗口：确认不补投
	if count.Load() != 2 {
		t.Fatalf("pre-start events must not be delivered, got %d deliveries", count.Load())
	}
}

// 广播语义：每个活跃实例各自独立全收（ADR 0006）。
func testBroadcastAllInstances(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("bc-all-" + t.Name())
	pub := event.NewPublisher(d)

	var c1, c2 atomic.Int64
	run1 := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	run2 := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run1, func(context.Context, event.Envelope[suitePayload]) error {
		c1.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}
	if err := ev.SubscribeBroadcast(run2, func(context.Context, event.Envelope[suitePayload]) error {
		c2.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}
	if err := run1.Start(ctx); err != nil {
		t.Fatalf("start 1: %v", err)
	}
	defer func() { _ = run1.Stop(context.WithoutCancel(ctx)) }()
	if err := run2.Start(ctx); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	defer func() { _ = run2.Stop(context.WithoutCancel(ctx)) }()

	for i := range 3 {
		mustPublish(t, ctx, pub, ev, suitePayload{Value: string(rune('a' + i))})
	}
	waitFor(t, 5*time.Second, "both instances receive all events", func() bool {
		return c1.Load() == 3 && c2.Load() == 3
	})
}

// 广播失败即跳过：handler 失败仅记日志跳过，不重试（ADR 0006）。
func testBroadcastFailureSkipped(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("bc-fail-" + t.Name())
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		count.Add(1)
		return context.DeadlineExceeded // 恒失败
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	mustPublish(t, ctx, pub, ev, suitePayload{"x"})
	waitFor(t, 5*time.Second, "failed event delivered once", func() bool {
		return count.Load() == 1
	})
	time.Sleep(200 * time.Millisecond) // 负向窗口：确认不重试
	if count.Load() != 1 {
		t.Fatalf("broadcast failure must not retry, got %d deliveries", count.Load())
	}
}

// 广播失败不阻塞后续事件：失败跳过后水位继续推进，后续事件正常投递
// （ADR 0006「不阻塞」）。
func testBroadcastFailureNotBlocking(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("bc-blk-" + t.Name())
	pub := event.NewPublisher(d)

	var bad, good atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		if e.Payload.Value == "bad" {
			bad.Add(1)
			return context.DeadlineExceeded
		}
		good.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	mustPublish(t, ctx, pub, ev, suitePayload{"bad"})
	mustPublish(t, ctx, pub, ev, suitePayload{"good"})
	waitFor(t, 5*time.Second, "failure skipped and subsequent delivered", func() bool {
		return bad.Load() == 1 && good.Load() == 1
	})
}

// 广播水位单调、无重复投递（ADR 0004）。
func testBroadcastNoDuplicate(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("bc-dup-" + t.Name())
	pub := event.NewPublisher(d)

	var mu sync.Mutex
	seen := map[string]int{}
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		mu.Lock()
		seen[e.ID]++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	for i := range 3 {
		mustPublish(t, ctx, pub, ev, suitePayload{Value: string(rune('a' + i))})
	}
	waitFor(t, 5*time.Second, "first burst delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	})
	for i := range 2 {
		mustPublish(t, ctx, pub, ev, suitePayload{Value: string(rune('x' + i))})
	}
	waitFor(t, 5*time.Second, "second burst delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 5
	})
	time.Sleep(100 * time.Millisecond) // 负向窗口：确认无重复
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 5 {
		t.Fatalf("want 5 unique events, got %d", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("watermark must be monotonic: event %s delivered %d times", id, n)
		}
	}
}

// 无参事件广播投递：NoPayload 载荷经 codec 编解码后正常送达
// （design.md §3 的 2×2 正交：无参 × 广播）。
func testNoPayloadBroadcast(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[event.NoPayload]("np-bc-" + t.Name())
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(discardLogger()))
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[event.NoPayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := pub.PublishNoPayload(ctx, ev); err != nil {
		t.Fatalf("publish no-payload: %v", err)
	}
	waitFor(t, 5*time.Second, "no-payload event delivered", func() bool {
		return count.Load() == 1
	})
}

// 无参事件认领消费：NoPayload 载荷处理并置 done，不重投
// （design.md §3 的 2×2 正交：无参 × 认领）。
func testNoPayloadClaim(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[event.NoPayload]("np-claim-" + t.Name())
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("np"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[event.NoPayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := pub.PublishNoPayload(ctx, ev); err != nil {
		t.Fatalf("publish no-payload: %v", err)
	}
	waitFor(t, 5*time.Second, "no-payload event claimed", func() bool {
		return count.Load() == 1
	})
	time.Sleep(200 * time.Millisecond) // 负向窗口：确认完成后不重投
	if count.Load() != 1 {
		t.Fatalf("completed no-payload event must not be redelivered, got %d", count.Load())
	}
}

// 类型间不互相饿死：A 类型洪泛且每轮限额 1 时，B 类型仍按自己的节奏
// 被独立 poller 投递，不被 A 的洪泛阻塞（ADR 0004）。
func testFetchLimitNoStarvation(t *testing.T, d event.Driver) {
	ctx := t.Context()
	pub := event.NewPublisher(d)
	evA := event.Declare[suitePayload]("starve-a-" + t.Name())
	evB := event.Declare[suitePayload]("starve-b-" + t.Name())

	var countA, countB atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(50*time.Millisecond),
		event.WithLogger(discardLogger()))
	if err := evA.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		countA.Add(1)
		return nil
	}, event.WithFetchLimit(1)); err != nil {
		t.Fatalf("subscribe A: %v", err)
	}
	if err := evB.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		countB.Add(1)
		return nil
	}, event.WithFetchLimit(1)); err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	// A 洪泛 30 条：每轮限额 1，需 30 轮 ≈1.5s；B 单条须在远早于
	// A 排干的时间内送达（共享串行且 A 优先的实现无法通过此窗口）。
	const flood = 30
	for range flood {
		mustPublish(t, ctx, pub, evA, suitePayload{Value: "flood"})
	}
	mustPublish(t, ctx, pub, evB, suitePayload{"lonely"})

	waitFor(t, 1*time.Second, "type B delivered despite type A flood", func() bool {
		return countB.Load() == 1
	})
	waitFor(t, 10*time.Second, "flooded type fully drained", func() bool {
		return countA.Load() == flood
	})
}

// 发布侧填充的信封字段（ID、元数据、OccurredAt）经 handler 原样送达
// （design.md §4：ID 是幂等去重与追踪主键，元数据透传）。
func testEnvelopeFieldsReachHandler(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("env-" + t.Name())
	pub := event.NewPublisher(d, event.WithIDGenerator(func() string { return "env-fixed-id" }))

	var mu sync.Mutex
	var gotID, gotTrace string
	var gotOccurred time.Time
	delivered := make(chan struct{})
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("env"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		mu.Lock()
		gotID, gotTrace, gotOccurred = e.ID, e.Metadata["trace_id"], e.OccurredAt
		mu.Unlock()
		close(delivered)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	mustPublish(t, ctx, pub, ev, suitePayload{"m"},
		event.WithMetadata(map[string]string{"trace_id": "tr-env"}))
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting: envelope delivered to handler")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotID != "env-fixed-id" {
		t.Fatalf("injected ID must reach handler, got %q", gotID)
	}
	if gotTrace != "tr-env" {
		t.Fatalf("metadata must reach handler, got trace_id %q", gotTrace)
	}
	if gotOccurred.IsZero() {
		t.Fatal("OccurredAt must be filled by publisher")
	}
}

// 认领端到端：认领 → handler 成功 → Complete 置 done → 不再重投。
func testClaimEndToEnd(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("claim-e2e-" + t.Name())
	pub := event.NewPublisher(d)
	mustPublish(t, ctx, pub, ev, suitePayload{"task"})

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("e2e"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		if e.Payload.Value != "task" {
			t.Errorf("payload mismatch: %+v", e.Payload)
		}
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitFor(t, 5*time.Second, "claim handler invoked", func() bool {
		return count.Load() == 1
	})
	time.Sleep(200 * time.Millisecond) // 负向窗口：确认完成后不重投
	if count.Load() != 1 {
		t.Fatalf("completed event must not be redelivered, got %d", count.Load())
	}
	// Driver 侧验证终态：done 后不可认领。
	ls, err := d.Claim(ctx, event.ClaimQuery{
		Type: ev.Name, ConsumerType: "e2e", Lease: time.Minute, Limit: 10,
		RetryLimit: 5, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(ls) != 0 {
		t.Fatalf("completed event must be done, got %+v", ls)
	}
}

// 认领失败自动重试：失败按退避重认领，成功后置 done（design.md §5.3）。
func testClaimRetryUntilSuccess(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("claim-retry-" + t.Name())
	pub := event.NewPublisher(d)
	mustPublish(t, ctx, pub, ev, suitePayload{"flaky"})

	var calls atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("retrier"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		if calls.Add(1) <= 2 {
			return context.Canceled // 前两次失败
		}
		return nil // 第三次成功
	},
		event.WithRetryLimit(5),
		event.WithBackoff(event.LinearBackoff(5*time.Millisecond, time.Hour)),
		event.WithLease(time.Second),
		event.WithHeartbeat(200*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitFor(t, 5*time.Second, "handler succeeds on third attempt", func() bool {
		return calls.Load() == 3
	})
	time.Sleep(200 * time.Millisecond) // 负向窗口：确认成功后停止重试
	if calls.Load() != 3 {
		t.Fatalf("success must stop retries, got %d calls", calls.Load())
	}
}

// 重试耗尽进入终态：handler 恒失败达 WithRetryLimit 后事件置 failed，
// 不再重试、Driver 侧不可再认领（ADR 0006）。
func testClaimRetryExhausted(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("exhaust-" + t.Name())
	pub := event.NewPublisher(d)
	mustPublish(t, ctx, pub, ev, suitePayload{"poison"})

	var calls atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("exhauster"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		calls.Add(1)
		return context.DeadlineExceeded
	},
		event.WithRetryLimit(2),
		event.WithBackoff(event.LinearBackoff(5*time.Millisecond, time.Hour)),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitFor(t, 5*time.Second, "retries exhausted at limit", func() bool {
		return calls.Load() == 2
	})
	time.Sleep(200 * time.Millisecond) // 负向窗口：确认不再重试
	if calls.Load() != 2 {
		t.Fatalf("exhausted event must not be retried, got %d calls", calls.Load())
	}
	// Driver 侧验证终态：failed 不可再认领。
	ls, err := d.Claim(ctx, event.ClaimQuery{
		Type: ev.Name, ConsumerType: "exhauster", Lease: time.Minute, Limit: 10,
		RetryLimit: 5, Now: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(ls) != 0 {
		t.Fatalf("exhausted event must be failed terminal, got %+v", ls)
	}
}

// 挂死 handler：超过最大处理时长停止续租，租约到期由其他实例接管
// （ADR 0006）。接管计入重试的 Attempts 递增由 Driver 级
// ExpiredLease_Claim_TakeoverIncrementsAttempts 契约验证；本测试只验证
// 端到端接管可达。
func testTakeoverAfterMaxProcessing(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("takeover-" + t.Name())
	pub := event.NewPublisher(d)
	mustPublish(t, ctx, pub, ev, suitePayload{"hung"})

	release := make(chan struct{})
	var entered1, entered2 atomic.Bool
	runnerOpts := []event.RunnerOption{
		event.WithPollInterval(5 * time.Millisecond),
		event.WithIdentity("taker"),
		event.WithLogger(discardLogger()),
	}
	run1 := event.NewRunner(d, runnerOpts...)
	if err := ev.SubscribeClaim(run1, func(context.Context, event.Envelope[suitePayload]) error {
		entered1.Store(true)
		<-release // 挂死：不返回
		return nil
	},
		event.WithLease(100*time.Millisecond),
		event.WithHeartbeat(30*time.Millisecond),
		event.WithMaxProcessing(150*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}
	if err := run1.Start(ctx); err != nil {
		t.Fatalf("start 1: %v", err)
	}

	waitFor(t, 5*time.Second, "runner1 claims and hangs", entered1.Load)
	time.Sleep(500 * time.Millisecond) // 超过最大处理时长，续租停止、租约到期

	// 第二个实例接管。
	run2 := event.NewRunner(d, runnerOpts...)
	if err := ev.SubscribeClaim(run2, func(context.Context, event.Envelope[suitePayload]) error {
		entered2.Store(true)
		return nil
	},
		event.WithLease(100*time.Millisecond),
		event.WithHeartbeat(30*time.Millisecond),
		event.WithMaxProcessing(150*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}
	if err := run2.Start(ctx); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	waitFor(t, 5*time.Second, "runner2 takes over", entered2.Load)

	close(release)
	_ = run1.Stop(context.WithoutCancel(ctx))
	_ = run2.Stop(context.WithoutCancel(ctx))

	// Driver 侧验证：run2 成功后事件为 done，不可再认领。
	ls, err := d.Claim(ctx, event.ClaimQuery{
		Type: ev.Name, ConsumerType: "taker", Lease: time.Minute, Limit: 10,
		RetryLimit: 5, Now: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(ls) != 0 {
		t.Fatalf("completed by run2 must be done, got %+v", ls)
	}
}

// 优雅停机：Stop 阻塞至在途 handler 跑完；在途 handler 的 ctx 不因
// 停机取消（design.md §9.1「停领在途跑完」——停机不把在途事件记为失败）。
func testGracefulStop(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("graceful-" + t.Name())
	pub := event.NewPublisher(d)
	mustPublish(t, ctx, pub, ev, suitePayload{"inflight"})

	var finished, entered, ctxCancelled atomic.Bool
	release := make(chan struct{})
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("gs"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(hctx context.Context, _ event.Envelope[suitePayload]) error {
		entered.Store(true)
		select {
		case <-release:
		case <-hctx.Done():
			ctxCancelled.Store(true) // 停机不得取消在途 handler
		}
		finished.Store(true)
		return nil
	}, event.WithLease(30*time.Second), event.WithHeartbeat(100*time.Millisecond)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, 5*time.Second, "handler in flight", entered.Load)

	stopErr := make(chan error, 1)
	go func() { stopErr <- run.Stop(context.WithoutCancel(ctx)) }()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-stopErr:
		t.Fatal("Stop must wait for in-flight handler")
	default:
	}

	close(release) // 放行 handler
	select {
	case err := <-stopErr:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after handler finished")
	}
	if !finished.Load() {
		t.Fatal("Stop returned before handler finished")
	}
	if ctxCancelled.Load() {
		t.Fatal("in-flight handler ctx must not be cancelled by Stop")
	}
}

// 停机后不再认领新事件（design.md §9.1）。
func testClaimNotResumedAfterStop(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("after-stop-" + t.Name())
	pub := event.NewPublisher(d)

	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("as"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := run.Stop(context.WithoutCancel(ctx)); err != nil {
		t.Fatalf("stop: %v", err)
	}

	mustPublish(t, ctx, pub, ev, suitePayload{"after-stop"})
	time.Sleep(300 * time.Millisecond) // 负向窗口：确认停机后不认领
	ls, err := d.Claim(ctx, event.ClaimQuery{
		Type: ev.Name, ConsumerType: "as", Lease: time.Minute, Limit: 10,
		RetryLimit: 5, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(ls) != 1 {
		t.Fatalf("event published after stop must remain unclaimed, got %+v", ls)
	}
}

// 就近唤醒：同进程发布触发本实例 poller 立即拉取，不必等待轮询间隔；
// 仅信号，读取仍走后端存储（ADR 0003）。
func testLocalWake(t *testing.T, d event.Driver) {
	ctx := t.Context()
	wake := event.NewLocalWake()
	pub := event.NewPublisher(d, event.WithLocalWake(wake))

	evB := event.Declare[suitePayload]("wake-bc-" + t.Name())
	evC := event.Declare[suitePayload]("wake-claim-" + t.Name())

	var bcCount, claimCount atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(time.Minute), // 轮询间隔内不靠 poll
		event.WithIdentity("wake"), event.WithLocalWake(wake), event.WithLogger(discardLogger()))
	if err := evB.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		bcCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	if err := evC.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		claimCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe C: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	mustPublish(t, ctx, pub, evB, suitePayload{"b"})
	mustPublish(t, ctx, pub, evC, suitePayload{"c"})
	waitFor(t, 3*time.Second, "wake signals trigger immediate delivery", func() bool {
		return bcCount.Load() == 1 && claimCount.Load() == 1
	})
}

// 订阅守卫：广播订阅传入认领专属选项返回 ErrNotClaimSubscription
// （docs/api.md §2.7）。
func testGuardClaimOnlyOnBroadcast(t *testing.T, d event.Driver) {
	ev := event.Declare[suitePayload]("guard-co-" + t.Name())
	handler := func(context.Context, event.Envelope[suitePayload]) error { return nil }
	run := event.NewRunner(d, event.WithLogger(discardLogger()))

	if err := ev.SubscribeBroadcast(run, handler, event.WithLease(time.Second)); err != event.ErrNotClaimSubscription {
		t.Fatalf("WithLease on broadcast: want ErrNotClaimSubscription, got %v", err)
	}
	if err := ev.SubscribeBroadcast(run, handler, event.WithConsumerType("x")); err != event.ErrNotClaimSubscription {
		t.Fatalf("WithConsumerType on broadcast: want ErrNotClaimSubscription, got %v", err)
	}
}

// 订阅守卫：认领订阅缺身份（订阅级与 Runner 默认皆缺）返回
// ErrIdentityMissing（docs/api.md §2.7）。
func testGuardMissingIdentity(t *testing.T, d event.Driver) {
	ev := event.Declare[suitePayload]("guard-id-" + t.Name())
	handler := func(context.Context, event.Envelope[suitePayload]) error { return nil }
	run := event.NewRunner(d, event.WithLogger(discardLogger())) // 无默认身份

	if err := ev.SubscribeClaim(run, handler); err != event.ErrIdentityMissing {
		t.Fatalf("want ErrIdentityMissing, got %v", err)
	}
}

// 订阅守卫：Start 后订阅返回 ErrRunnerStarted（docs/api.md §2.7）。
func testGuardSubscribeAfterStart(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("guard-ss-" + t.Name())
	handler := func(context.Context, event.Envelope[suitePayload]) error { return nil }

	run := event.NewRunner(d, event.WithIdentity("base"), event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := ev.SubscribeBroadcast(run, handler); err != event.ErrRunnerStarted {
		t.Fatalf("broadcast after start: want ErrRunnerStarted, got %v", err)
	}
	if err := ev.SubscribeClaim(run, handler); err != event.ErrRunnerStarted {
		t.Fatalf("claim after start: want ErrRunnerStarted, got %v", err)
	}
}
