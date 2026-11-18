package pgxc_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/pgxc"
)

// 单测共用：丢弃日志。
func discardSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// wakeProbeDriver 统计 Fetch 轮次的替身 Driver：跨实例唤醒的可见效果是
// 「通知到达触发本实例已挂起的 Fetch/唤醒等待立即拉取」（ADR 0013），
// 以 Fetch 调用次数断言桥接生效，不触库。
type wakeProbeDriver struct {
	event.Driver
	fetches atomic.Int64
}

func (d *wakeProbeDriver) Fetch(context.Context, event.FetchQuery) ([]event.RawEnvelope, error) {
	d.fetches.Add(1)
	return nil, nil
}

func (d *wakeProbeDriver) HeadPosition(context.Context, string) (string, error) {
	return "", nil
}

// 桥接分发：模拟 NOTIFY 到达后，绑定的 LocalWake 触发本实例订阅的
// poller 立即拉取（绕过 LISTEN 连接，直接驱动分发路径）。
func TestBoundWake_NotificationArrives_TriggersLocalFetch(t *testing.T) {
	probe := &wakeProbeDriver{}
	wake := event.NewLocalWake()
	w := pgxc.NewWake(nil, pgxc.WithSchema("unit_wake"), pgxc.WithLogger(discardSlog()))
	defer w.Close() // nil 池：监听未启动，Close 无操作
	w.Bind(wake)

	ctx := t.Context()
	run := event.NewRunner(probe,
		event.WithPollInterval(time.Hour), // 轮询间隔内不靠 poll
		event.WithLocalWake(wake), event.WithLogger(discardSlog()))
	ev := event.Declare[event.NoPayload]("unit-wake-x")
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[event.NoPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	if n := probe.fetches.Load(); n != 0 {
		t.Fatalf("fetch count before notification = %d, want 0", n)
	}
	pgxc.NotifyForTest(w, "unit_wake")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if probe.fetches.Load() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("notification must trigger an immediate local fetch via the bound LocalWake")
}

// 未桥接时信号静默丢弃（不 panic、不投递）；重绑后一次生效。
func TestUnboundWake_Dispatch_SilentDropThenRebindWorks(t *testing.T) {
	probe := &wakeProbeDriver{}
	wake := event.NewLocalWake()
	w := pgxc.NewWake(nil, pgxc.WithSchema("unit_wake"), pgxc.WithLogger(discardSlog()))
	defer w.Close()

	pgxc.NotifyForTest(w, "unit_wake") // 未 Bind：丢弃

	ctx := t.Context()
	run := event.NewRunner(probe,
		event.WithPollInterval(time.Hour),
		event.WithLocalWake(wake), event.WithLogger(discardSlog()))
	ev := event.Declare[event.NoPayload]("unit-wake-rebind")
	if err := ev.SubscribeBroadcast(run, func(context.Context, event.Envelope[event.NoPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	w.Bind(wake) // 重绑（后一次生效）
	pgxc.NotifyForTest(w, "unit_wake")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if probe.fetches.Load() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("signal after Bind must reach the local subscriptions")
}

// 非法 schema：监听不启动（信号退化由轮询兜底），构造与 Close 不 panic。
func TestInvalidSchemaWake_NotStarted_NoPanic(t *testing.T) {
	w := pgxc.NewWake(nil, pgxc.WithSchema("bad-name"), pgxc.WithLogger(discardSlog()))
	w.Bind(event.NewLocalWake())
	pgxc.NotifyForTest(w, "go_event_wake_bad-name")
	w.Close()
	w.Close() // Close 幂等
}

// 通道名超 Postgres 标识上限：监听不启动（信号退化，ADR 0013 §3）。
func TestOverlongChannelWake_NotStarted_NoPanic(t *testing.T) {
	long := repeatSchema(56) // 前缀 14 字节 + 56 > 63
	w := pgxc.NewWake(nil, pgxc.WithSchema(long), pgxc.WithLogger(discardSlog()))
	w.Bind(event.NewLocalWake())
	pgxc.NotifyForTest(w, "x")
	w.Close()
}

func repeatSchema(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
