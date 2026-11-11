package event_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type runPayload struct {
	Value string `json:"value"`
}

// HeadPosition 失败：Start 返回错误、不启动；修复后重试 Start 成功
// （广播基线在 Start 同步确立，ADR 0006）。
func TestHeadPositionFailure_Start_ReturnsErrorAndRetryable(t *testing.T) {
	d := newStubDriver()
	d.mu.Lock()
	d.headFail = errStub
	d.mu.Unlock()

	ev := event.Declare[runPayload]("unit.baseline")
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := run.Start(context.Background()); err == nil {
		t.Fatal("Start must fail when baseline query fails")
	}
	// 失败的 Start 不置 started 标记：订阅仍被接受，可修复后重启。
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("failed Start must not mark runner started, got %v", err)
	}

	d.mu.Lock()
	d.headFail = nil
	d.mu.Unlock()
	if err := run.Start(context.Background()); err != nil {
		t.Fatalf("retry Start after failure must succeed, got %v", err)
	}
	if err := run.Stop(context.WithoutCancel(context.Background())); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// Fetch 失败：poller 记日志继续轮询，恢复后照常投递（api.md §2.4：
// 错误视为该次操作失败，core 重试或跳过）。
func TestFetchFailure_Poll_KeepsPollingUntilSuccess(t *testing.T) {
	d := newStubDriver()
	d.mu.Lock()
	d.fetchErrs = 3 // 前 3 轮注入失败
	d.mu.Unlock()

	ctx := context.Background()
	ev := event.Declare[runPayload]("unit.fetcherr")
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if err := ev.Publish(ctx, pub, runPayload{"x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitForUnit(t, 5*time.Second, "event delivered after transient fetch failures", func() bool {
		return count.Load() == 1
	})
}

// handler panic：转为 error，广播失败跳过，poller 与后续投递不受影响
// （callHandler 的 panic 转换语义）。
func TestHandlerPanic_Broadcast_SkipsAndContinues(t *testing.T) {
	d := newStubDriver()
	ctx := context.Background()
	ev := event.Declare[runPayload]("unit.panic")
	pub := event.NewPublisher(d)

	var count atomic.Int64
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeBroadcast(run, func(_ context.Context, e event.Envelope[runPayload]) error {
		if e.Payload.Value == "boom" {
			panic("handler exploded")
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

	if err := ev.Publish(ctx, pub, runPayload{"boom"}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := ev.Publish(ctx, pub, runPayload{"fine"}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	waitForUnit(t, 5*time.Second, "non-panicking event delivered after panic skip", func() bool {
		return count.Load() == 1
	})
}
