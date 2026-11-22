package event_test

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

// 启动后动态注册（ADR 0015）：注册入口全生命周期可用，启动后注册即时
// 生效；消费语义（广播基线锚定于注册、认领从头重放）由 eventtest 契约
// 套件在真实后端上钉死，此处钉守卫与失败回报的单元事实。

// 动态注册广播订阅：基线锚定于注册调用——注册前发布的事件不投递，
// 注册后发布的必投（与 Start 基线同构）。
func TestPostStartSubscribe_Broadcast_OnlyPostRegistrationDelivered(t *testing.T) {
	d := newStubDriver()
	ctx := context.Background()
	ev := event.Declare[runPayload]("unit.dynbc")
	pub := event.NewPublisher(d)

	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := ev.Publish(ctx, pub, runPayload{"pre"}); err != nil {
		t.Fatalf("publish pre: %v", err)
	}

	var deliveries atomic.Int64
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		deliveries.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("dynamic subscribe: %v", err)
	}

	if err := ev.Publish(ctx, pub, runPayload{"post"}); err != nil {
		t.Fatalf("publish post: %v", err)
	}
	waitForUnit(t, 2*time.Second, "post-registration delivery", func() bool {
		return deliveries.Load() == 1
	})
	// 注册前发布的事件按设计跳过：投递总数恰为注册后的一次。
	if deliveries.Load() != 1 {
		t.Fatalf("want exactly 1 delivery (post), got %d", deliveries.Load())
	}
}

// 动态注册广播订阅基线失败：错误返回（包装底层错误）、订阅不生效，
// 失败期间发布的事件不补投；修复后重试注册成功，基线锚在失败期间已
// 发布的事件之上（与 Start 基线失败语义同构）。
func TestPostStartSubscribe_BroadcastBaselineFailure_RejectedRetryable(t *testing.T) {
	d := newStubDriver()
	ctx := context.Background()
	ev := event.Declare[runPayload]("unit.dynfail")
	pub := event.NewPublisher(d)

	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	var mu sync.Mutex
	var seen []string
	handler := func(_ context.Context, e event.Envelope[runPayload]) error {
		mu.Lock()
		seen = append(seen, e.Payload.Value)
		mu.Unlock()
		return nil
	}

	d.mu.Lock()
	d.headFail = errStub
	d.mu.Unlock()
	if err := ev.SubscribeBroadcast(run, handler); !errors.Is(err, errStub) {
		t.Fatalf("want wrapped stub error, got %v", err)
	}

	// 订阅不生效：注册失败期间发布的事件不投递。
	if err := ev.Publish(ctx, pub, runPayload{"during-failure"}); err != nil {
		t.Fatalf("publish during failure: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	quiet := len(seen) == 0
	mu.Unlock()
	if !quiet {
		t.Fatalf("failed registration must not take effect, got deliveries %v", seen)
	}

	d.mu.Lock()
	d.headFail = nil
	d.mu.Unlock()
	if err := ev.SubscribeBroadcast(run, handler); err != nil {
		t.Fatalf("retry subscribe: %v", err)
	}

	// 重试注册的基线锚定在失败期间已发布的事件之上：不补投。
	if err := ev.Publish(ctx, pub, runPayload{"post"}); err != nil {
		t.Fatalf("publish post: %v", err)
	}
	waitForUnit(t, 2*time.Second, "post delivery after retry", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(seen, "post")
	})
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen, []string{"post"}) {
		t.Fatalf("want only post delivered, got %v", seen)
	}
}

// Stop 后注册一律拒绝（Runner 一次性，ADR 0015）。
func TestSubscribeAfterStop_RejectedErrRunnerStopped(t *testing.T) {
	d := newStubDriver()
	ctx := context.Background()
	ev := event.Declare[runPayload]("unit.dynstop")
	run := event.NewRunner(d, event.WithIdentity("base"), event.WithLogger(quietLogger()))

	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := run.Stop(context.WithoutCancel(ctx)); err != nil {
		t.Fatalf("stop: %v", err)
	}

	handler := func(context.Context, event.Envelope[runPayload]) error { return nil }
	if err := ev.SubscribeBroadcast(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("broadcast after stop: want ErrRunnerStopped, got %v", err)
	}
	if err := ev.SubscribeClaim(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("claim after stop: want ErrRunnerStopped, got %v", err)
	}
}

// Start ctx 取消后（poller 已退出、未 Stop）注册同样拒绝：runCtx 守卫把
// 「已停转」的 Runner 与已 Stop 一致对待，避免注册出永不消费的死订阅。
func TestSubscribeAfterStartCtxCancel_RejectedErrRunnerStopped(t *testing.T) {
	d := newStubDriver()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ev := event.Declare[runPayload]("unit.dynctx")
	run := event.NewRunner(d, event.WithIdentity("base"), event.WithLogger(quietLogger()))

	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	cancel()
	handler := func(context.Context, event.Envelope[runPayload]) error { return nil }
	if err := ev.SubscribeBroadcast(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("broadcast after ctx cancel: want ErrRunnerStopped, got %v", err)
	}
	if err := ev.SubscribeClaim(run, handler); err != event.ErrRunnerStopped {
		t.Fatalf("claim after ctx cancel: want ErrRunnerStopped, got %v", err)
	}
	_ = run.Stop(context.WithoutCancel(context.Background()))
}

// 动态注册与 Start/Stop 并发：-race 下无死锁、无 panic；运行期注册全部
// 生效（poller 由 Stop 统一收回），Stop 后注册以 ErrRunnerStopped 退出。
func TestSubscribe_ConcurrentWithStartStop_NoDeadlock(t *testing.T) {
	d := newStubDriver()
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))

	ev := event.Declare[runPayload]("unit.dynrace")
	handler := func(context.Context, event.Envelope[runPayload]) error { return nil }

	const workers = 4
	stop := make(chan struct{})
	abnormal := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := ev.SubscribeBroadcast(run, handler)
				if err != nil {
					if !errors.Is(err, event.ErrRunnerStopped) {
						select {
						case abnormal <- err:
						default:
						}
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	ctx := context.Background()
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	if err := run.Stop(context.WithoutCancel(ctx)); err != nil {
		t.Fatalf("stop: %v", err)
	}
	wg.Wait()

	select {
	case err := <-abnormal:
		t.Fatalf("unexpected registration error: %v", err)
	default:
	}
}
