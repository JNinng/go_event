package eventtest

// 启动后动态注册订阅（ADR 0015）：注册入口在 Runner 生命周期内（Stop 前）
// 全部可用，启动后注册即时生效。两种消费模式的起点语义在此钉死——广播
// 基线锚定于注册调用（只处理注册之后发布的事件，与 Start 基线同构）；
// 认领对齐 design.md §5.1 新身份口径（从头认领、限保留窗口）。基线失败
// 注入经测试内 Driver 装饰器完成，套件不读环境变量。

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// errDynamicBaseline 基线失败注入用错误（errors.Is 可判别）。
var errDynamicBaseline = errors.New("eventtest: injected head position failure")

// headPositionFailureDriver 测试装饰器：按开关令 HeadPosition 失败，注入
// 动态注册的基线锚定失败；其余操作直通底层 Driver。
type headPositionFailureDriver struct {
	event.Driver
	mu       sync.Mutex
	failHead bool
}

func (d *headPositionFailureDriver) HeadPosition(ctx context.Context, typ string) (string, error) {
	d.mu.Lock()
	fail := d.failHead
	d.mu.Unlock()
	if fail {
		return "", errDynamicBaseline
	}
	return d.Driver.HeadPosition(ctx, typ)
}

// 动态注册广播订阅：基线锚定于注册调用——注册前发布的事件不投递，
// 注册后发布的事件必然投递（与 Start 基线同构，ADR 0006/0015）。
func testDynamicBroadcastOnlyAfterRegistration(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("dyn-bc-" + t.Name())
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	// 注册前发布：基线越过，不补投。
	mustPublish(t, ctx, pub, ev, suitePayload{"pre"})

	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("dynamic subscribe: %v", err)
	}

	// 注册返回即基线确立：之后发布必然可投递。
	mustPublish(t, ctx, pub, ev, suitePayload{"post"})
	waitFor(t, 5*time.Second, "post-registration event delivered", func() bool {
		return count.Load() == 1
	})
	time.Sleep(100 * time.Millisecond) // 负向窗口：确认不补投
	if count.Load() != 1 {
		t.Fatalf("pre-registration events must not be delivered, got %d deliveries", count.Load())
	}
}

// 动态注册认领订阅：起点对齐 §5.1 新身份口径——注册前的存量事件与注册
// 后发布的事件都投递（身份终态粒度、含其他身份已 done 事件的重放由
// NewConsumerType_Claim_ReplaysHistory 在 Driver 层钉死，ADR 0015）。
func testDynamicClaimReplaysBacklog(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("dyn-claim-" + t.Name())
	pub := event.NewPublisher(d)

	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("dyn-late"), event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	// Start 与注册之间发布的存量：新订阅从头认领。
	mustPublish(t, ctx, pub, ev, suitePayload{"backlog"})

	var mu sync.Mutex
	var seen []string
	if err := ev.SubscribeClaim(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		mu.Lock()
		seen = append(seen, e.Payload.Value)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("dynamic subscribe: %v", err)
	}

	mustPublish(t, ctx, pub, ev, suitePayload{"fresh"})
	waitFor(t, 5*time.Second, "backlog replay and fresh event delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(seen, "backlog") || !slices.Contains(seen, "fresh") {
		t.Fatalf("want backlog+fresh delivery, got %v", seen)
	}
}

// 动态注册广播订阅基线失败：错误返回（包装底层错误）、订阅不生效、既有
// 认领订阅不受影响；修复后重试注册成功，基线锚定在失败期间已发布的事件
// 之上、不补投（与 Start 基线失败语义同构，ADR 0015）。
func testDynamicBroadcastBaselineFailure_RejectedRetryable(t *testing.T, d event.Driver) {
	ctx := t.Context()
	wrapped := &headPositionFailureDriver{Driver: d}
	ev := event.Declare[suitePayload]("dyn-bcfail-" + t.Name())
	evClaim := event.Declare[suitePayload]("dyn-bcfail-claim-" + t.Name())
	pub := event.NewPublisher(wrapped)

	var claimCount atomic.Int64
	var postCount atomic.Int64
	postHandler := func(_ context.Context, e event.Envelope[suitePayload]) error {
		if e.Payload.Value == "post" {
			postCount.Add(1)
		}
		return nil
	}

	run := event.NewRunner(wrapped, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("dyn-fail"), event.WithLogger(discardLogger()))
	// 认领订阅不受广播基线失败影响：失败窗口内照常消费。
	if err := evClaim.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		claimCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe claim: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	wrapped.mu.Lock()
	wrapped.failHead = true
	wrapped.mu.Unlock()
	if err := ev.SubscribeBroadcast(run, postHandler); !errors.Is(err, errDynamicBaseline) {
		t.Fatalf("want injected baseline error, got %v", err)
	}

	// 失败期间照常发布：重试注册的基线将锚定在其上。
	mustPublish(t, ctx, pub, ev, suitePayload{"during-failure"})
	// 既有认领订阅不受影响。
	mustPublish(t, ctx, pub, evClaim, suitePayload{"claim-ok"})
	waitFor(t, 5*time.Second, "claim subscription unaffected by baseline failure", func() bool {
		return claimCount.Load() == 1
	})

	wrapped.mu.Lock()
	wrapped.failHead = false
	wrapped.mu.Unlock()
	if err := ev.SubscribeBroadcast(run, postHandler); err != nil {
		t.Fatalf("retry subscribe: %v", err)
	}

	mustPublish(t, ctx, pub, ev, suitePayload{"post"})
	waitFor(t, 5*time.Second, "post delivered after retry registration", func() bool {
		return postCount.Load() == 1
	})
	time.Sleep(100 * time.Millisecond) // 负向窗口：失败期间的事件不补投
	if postCount.Load() != 1 {
		t.Fatalf("during-failure event must not be delivered, got %d post deliveries", postCount.Load())
	}
}

// Stop 后注册一律拒绝：Runner 一次性，显式错误优于静默无效订阅（ADR 0015）。
func testSubscribeAfterStop_RejectedErrRunnerStopped(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("dyn-stop-" + t.Name())
	handler := func(context.Context, event.Envelope[suitePayload]) error { return nil }

	run := event.NewRunner(d, event.WithIdentity("base"), event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := run.Stop(context.WithoutCancel(ctx)); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if err := ev.SubscribeBroadcast(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("broadcast after stop: want ErrRunnerStopped, got %v", err)
	}
	if err := ev.SubscribeClaim(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("claim after stop: want ErrRunnerStopped, got %v", err)
	}
}

// 动态注册的订阅挂接就近唤醒：轮询间隔内不靠 poll，发布即投（ADR 0003）。
func testDynamicSubscriptionLocalWake(t *testing.T, d event.Driver) {
	ctx := t.Context()
	wake := event.NewLocalWake()
	pub := event.NewPublisher(d, event.WithLocalWake(wake))
	ev := event.Declare[suitePayload]("dyn-wake-" + t.Name())

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(time.Minute), // 间隔内不靠 poll
		event.WithLocalWake(wake), event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[suitePayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("dynamic subscribe: %v", err)
	}
	mustPublish(t, ctx, pub, ev, suitePayload{"wake"})
	waitFor(t, 3*time.Second, "wake-driven delivery to dynamically registered subscription", func() bool {
		return count.Load() == 1
	})
}

// 动态注册订阅的在途 handler 由 Stop 优雅等待：与 Start 前注册的订阅同
// 一停机契约（design.md §9.1）。
func testDynamicSubscriptionStopWaitsInFlight(t *testing.T, d event.Driver) {
	ctx := t.Context()
	ev := event.Declare[suitePayload]("dyn-stopwait-" + t.Name())
	pub := event.NewPublisher(d)

	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("dyn-inflight"), event.WithLogger(discardLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		close(started)
		<-release
		finished.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("dynamic subscribe: %v", err)
	}

	mustPublish(t, ctx, pub, ev, suitePayload{"in-flight"})
	waitFor(t, 5*time.Second, "handler entered", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})

	stopErr := make(chan error, 1)
	go func() { stopErr <- run.Stop(context.WithoutCancel(ctx)) }()
	select {
	case err := <-stopErr:
		t.Fatalf("stop returned before in-flight handler finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-stopErr; err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !finished.Load() {
		t.Fatal("in-flight handler must finish before stop returns")
	}
}
