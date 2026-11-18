package event_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

const (
	prefetchType     = "unit.prefetch"
	prefetchConsumer = "unit-prefetch-consumer"
)

// prefetchLease 构造一条可直接 decode 的租约（payload 为空对象）。
func prefetchLease(id string) event.Lease {
	return event.Lease{
		Envelope: event.RawEnvelope{
			ID:      id,
			Type:    prefetchType,
			Payload: []byte(`{}`),
		},
		ConsumerType: prefetchConsumer,
		LeaseToken:   "token-" + id,
		Attempts:     1,
	}
}

// claimProbeDriver 包装 stubDriver，记录每次 ClaimQuery，可编排返回的租约
// 批次、注入 Claim 错误，并留痕心跳与终态调用（认领预取单元测试替身）。
type claimProbeDriver struct {
	*stubDriver

	probeMu   sync.Mutex
	claimQs   []event.ClaimQuery
	claimErrs int // 剩余注入 Claim 错误的次数
	results   []event.ClaimResult
	beats     []event.HeartbeatRequest
	completes []event.CompleteRequest
	fails     []event.FailRequest

	claimSignal chan struct{} // 每次 Claim 非阻塞投递一次（容量 1）
}

func newClaimProbeDriver() *claimProbeDriver {
	return &claimProbeDriver{
		stubDriver:  newStubDriver(),
		claimSignal: make(chan struct{}, 1),
	}
}

// enqueueBatch 追加一批 Claim 返回的租约（计数为零的 ClaimResult）：
// 按调用顺序消耗，耗尽后返回空批次。
func (d *claimProbeDriver) enqueueBatch(leases ...event.Lease) {
	d.enqueueResult(event.ClaimResult{Leases: leases})
}

// enqueueResult 追加一次 Claim 返回的完整 ClaimResult（可编程接管/终态
// 计数，供观测指标断言）：按调用顺序消耗，耗尽后返回零值结果。
func (d *claimProbeDriver) enqueueResult(res event.ClaimResult) {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	d.results = append(d.results, res)
}

// failClaims 让接下来 n 次 Claim 返回 errStub。
func (d *claimProbeDriver) failClaims(n int) {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	d.claimErrs = n
}

func (d *claimProbeDriver) Claim(_ context.Context, q event.ClaimQuery) (event.ClaimResult, error) {
	d.probeMu.Lock()
	d.claimQs = append(d.claimQs, q)
	var out event.ClaimResult
	var err error
	if d.claimErrs > 0 {
		d.claimErrs--
		err = errStub
	} else if len(d.results) > 0 {
		out = d.results[0]
		d.results = d.results[1:]
	}
	d.probeMu.Unlock()

	select {
	case d.claimSignal <- struct{}{}:
	default:
	}
	return out, err
}

func (d *claimProbeDriver) Heartbeat(_ context.Context, h event.HeartbeatRequest) error {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	d.beats = append(d.beats, h)
	return nil
}

func (d *claimProbeDriver) Complete(_ context.Context, c event.CompleteRequest) error {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	d.completes = append(d.completes, c)
	return nil
}

func (d *claimProbeDriver) Fail(_ context.Context, f event.FailRequest) error {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	d.fails = append(d.fails, f)
	return nil
}

// failedRequests 已记录的 Fail 请求（保持调用顺序）。
func (d *claimProbeDriver) failedRequests() []event.FailRequest {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	return append([]event.FailRequest(nil), d.fails...)
}

func (d *claimProbeDriver) claimCount() int {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	return len(d.claimQs)
}

func (d *claimProbeDriver) claimLimits() []int {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	out := make([]int, len(d.claimQs))
	for i, q := range d.claimQs {
		out[i] = q.Limit
	}
	return out
}

func (d *claimProbeDriver) heartbeatCount(id string) int {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	n := 0
	for _, h := range d.beats {
		if h.EventID == id {
			n++
		}
	}
	return n
}

// completedEventIDs 已成功 Complete 的事件 ID（保持调用顺序）。
func (d *claimProbeDriver) completedEventIDs() []string {
	d.probeMu.Lock()
	defer d.probeMu.Unlock()
	out := make([]string, len(d.completes))
	for i, c := range d.completes {
		out[i] = c.EventID
	}
	return out
}

var _ event.Driver = (*claimProbeDriver)(nil)

// logWatcher 捕获 Runner 日志消息，用作停机放弃派发的显式同步点。
type logWatcher struct {
	msgs chan string
}

func newLogWatcher() *logWatcher { return &logWatcher{msgs: make(chan string, 32)} }

func (w *logWatcher) Enabled(context.Context, slog.Level) bool { return true }

func (w *logWatcher) Handle(_ context.Context, r slog.Record) error {
	select {
	case w.msgs <- r.Message:
	default:
	}
	return nil
}

func (w *logWatcher) WithAttrs([]slog.Attr) slog.Handler { return w }
func (w *logWatcher) WithGroup(string) slog.Handler      { return w }

// waitFor 等待一条指定内容的日志（超时即失败）。
func (w *logWatcher) waitFor(t *testing.T, msg string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case got := <-w.msgs:
			if got == msg {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting log message %q", msg)
		}
	}
}

// 认领有效批大小 = min(FetchLimit, 空闲并发槽位)；FetchLimit 对认领是
// 批次上限，不要求 ≤ Concurrency。
func TestClaimBatchSize_Claim_LimitIsMinOfFetchLimitAndFreeSlots(t *testing.T) {
	cases := []struct {
		name        string
		concurrency int
		fetchLimit  int
		wantLimit   int
	}{
		{"Concurrency1_FetchLimit100_LimitIs1", 1, 100, 1},
		{"Concurrency2_FetchLimit2_LimitIs2", 2, 2, 2},
		{"Concurrency4_FetchLimit2_LimitIs2", 4, 2, 2},
		{"Concurrency4_FetchLimit100_LimitIs4", 4, 100, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newClaimProbeDriver()
			ev := event.Declare[runPayload](prefetchType)
			// 大轮询间隔：只观察启动后的首轮认领。
			run := event.NewRunner(d, event.WithPollInterval(time.Hour), event.WithLogger(quietLogger()))
			if err := ev.SubscribeClaim(run,
				func(context.Context, event.Envelope[runPayload]) error { return nil },
				event.WithConsumerType(prefetchConsumer),
				event.WithConcurrency(tc.concurrency),
				event.WithFetchLimit(tc.fetchLimit),
			); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			if err := run.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() { _ = run.Stop(context.WithoutCancel(context.Background())) }()

			waitForUnit(t, 2*time.Second, "first claim issued", func() bool {
				return d.claimCount() >= 1
			})
			if got := d.claimLimits()[0]; got != tc.wantLimit {
				t.Fatalf("first Claim Limit = %d, want %d", got, tc.wantLimit)
			}
		})
	}
}

// 缺省选项（不传 WithConcurrency/WithFetchLimit）下首轮 Limit 为 1：
// 缺省 FetchLimit=100 > Concurrency=1，认领被空闲槽位封顶。
func TestDefaultOptions_Claim_LimitEqualsOne(t *testing.T) {
	d := newClaimProbeDriver()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(time.Hour), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error { return nil },
		event.WithConsumerType(prefetchConsumer),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(context.Background())) }()

	waitForUnit(t, 2*time.Second, "first claim issued", func() bool {
		return d.claimCount() >= 1
	})
	if got := d.claimLimits()[0]; got != 1 {
		t.Fatalf("default first Claim Limit = %d, want 1", got)
	}
}

// 处理中部分空闲：Concurrency=2、首批 1 条且 handler 阻塞时，下一轮 Claim
// 的 Limit 精确等于剩余空闲槽位 1，且第二次认领发生在 handler 仍阻塞期间。
func TestPartialSlotsBusy_NextPoll_ClaimLimitEqualsFreeSlots(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("partial-1"))

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var finished atomic.Bool

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			startOnce.Do(func() { close(started) })
			<-release
			finished.Store(true)
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(2),
		event.WithFetchLimit(100),
		event.WithLease(time.Second),
		event.WithHeartbeat(100*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		close(release)
		_ = run.Stop(context.WithoutCancel(ctx))
	}()

	waitForUnit(t, 2*time.Second, "first leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	waitForUnit(t, 2*time.Second, "second claim issued while first handler blocked", func() bool {
		return d.claimCount() >= 2
	})
	if got := d.claimLimits()[1]; got != 1 {
		t.Fatalf("second Claim Limit = %d, want free slots 1", got)
	}
	if finished.Load() {
		t.Fatal("first handler must still be blocked when second claim happens")
	}
}

// 槽位释放即触发补认领：轮询间隔设为 1 小时，handler 结束后第二次 Claim
// 只能来自补认领信号，积压消化不依赖轮询间隔。
func TestSlotReleased_Claim_RefillsWithoutWaitingPollInterval(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("refill-1"))

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	stopRelease := func() { releaseOnce.Do(func() { close(release) }) }

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(time.Hour), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			startOnce.Do(func() { close(started) })
			<-release
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(100),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		stopRelease()
		_ = run.Stop(context.WithoutCancel(ctx))
	}()

	waitForUnit(t, 2*time.Second, "first leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if got := d.claimCount(); got != 1 {
		t.Fatalf("claim calls before release = %d, want 1", got)
	}

	stopRelease()
	waitForUnit(t, 2*time.Second, "refill claim after slot release", func() bool {
		return d.claimCount() >= 2
	})
	if got := d.claimLimits()[1]; got != 1 {
		t.Fatalf("refill Claim Limit = %d, want free slots 1", got)
	}
}

// 所有槽位被占用期间不再发起新的 Claim：不存在「认领后排队等槽位」的
// 第二批租约。
func TestAllSlotsBusy_Poll_DoesNotClaim(t *testing.T) {
	const pollInterval = 5 * time.Millisecond

	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("busy-1"))

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(pollInterval), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			startOnce.Do(func() { close(started) })
			<-release // 占住唯一槽位，直到断言完成
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(100),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		close(release)
		_ = run.Stop(context.WithoutCancel(ctx))
	}()

	waitForUnit(t, 2*time.Second, "leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})

	claims := d.claimCount()
	<-d.claimSignal // 丢弃首批认领信号；此后任意新 Claim 都代表违约
	// 轮询周期下界的负向观察窗口：至少 20 个轮询周期，期间若有 Claim 即违约。
	select {
	case <-d.claimSignal:
		t.Fatalf("poll issued another Claim while all slots busy: limits=%v", d.claimLimits())
	case <-time.After(20 * pollInterval):
	}
	if got := d.claimCount(); got != claims {
		t.Fatalf("claim calls = %d while all slots busy, want %d", got, claims)
	}
}

// 派发即启动心跳，且 handler 阻塞期间持续续租：Concurrency=2 但首批只返回
// 1 条，断言该租约在 handler 阻塞期间至少收到 2 次心跳。
func TestHandlerBlockedRenewal_Heartbeat_RepeatsWhileProcessing(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("hb-1"))

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	// 大轮询间隔：只观察首轮认领派发后的心跳。
	run := event.NewRunner(d, event.WithPollInterval(time.Hour), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			startOnce.Do(func() { close(started) })
			<-release
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(2),
		event.WithFetchLimit(100),
		event.WithLease(2*time.Second),
		event.WithHeartbeat(5*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		close(release)
		_ = run.Stop(context.WithoutCancel(ctx))
	}()

	waitForUnit(t, 2*time.Second, "leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	waitForUnit(t, 3*time.Second, "blocked handler renewed at least twice", func() bool {
		return d.heartbeatCount("hb-1") >= 2
	})
	if got := d.heartbeatCount("hb-1"); got < 2 {
		t.Fatalf("heartbeat count for blocked handler = %d, want >= 2", got)
	}
}

// 停机时尚未派发（等不到槽位）的租约被放弃：不处理、不 Complete、不心跳。
// 用超过 Limit 的违约批次构造该场景；先 cancel 并以「停止放弃」日志确认
// stop-2 已放弃，再释放 handler，stop-2 的放弃结果因此是确定性的。
func TestStopUndispatched_Lease_AbandonedNotProcessed(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("stop-1"), prefetchLease("stop-2")) // 2 > Limit 1

	watcher := newLogWatcher()
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	stopRelease := func() { releaseOnce.Do(func() { close(release) }) }
	var handledMu sync.Mutex
	var handled []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(slog.New(watcher)))
	if err := ev.SubscribeClaim(run,
		func(_ context.Context, e event.Envelope[runPayload]) error {
			handledMu.Lock()
			handled = append(handled, e.ID)
			handledMu.Unlock()
			startOnce.Do(func() { close(started) })
			<-release
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(100),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		stopRelease()
		_ = run.Stop(context.WithoutCancel(context.Background()))
	}()

	waitForUnit(t, 2*time.Second, "first leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	// 顺序固定：cancel 后等 stop-2 被放弃的日志，再释放 stop-1。
	cancel()
	watcher.waitFor(t, "event: stopping, lease released for takeover")
	stopRelease()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := run.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	handledMu.Lock()
	gotHandled := append([]string(nil), handled...)
	handledMu.Unlock()
	if want := []string{"stop-1"}; !slices.Equal(gotHandled, want) {
		t.Fatalf("handled event IDs = %v, want %v", gotHandled, want)
	}
	if want := []string{"stop-1"}; !slices.Equal(d.completedEventIDs(), want) {
		t.Fatalf("completed event IDs = %v, want %v", d.completedEventIDs(), want)
	}
	if got := d.heartbeatCount("stop-2"); got != 0 {
		t.Fatalf("abandoned lease heartbeat count = %d, want 0", got)
	}
}

// 后端违约返回超过 Limit 的批次且不停机：第二条派发等待槽位，槽位释放后
// 继续处理（dispatchClaim 仅在 ctx.Done 时放弃派发）。
func TestOverLimitBatch_NoStop_SecondLeaseProcessedAfterRelease(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("ov-1"), prefetchLease("ov-2")) // 2 > Limit 1

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var handledMu sync.Mutex
	var handled []string

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(_ context.Context, e event.Envelope[runPayload]) error {
			handledMu.Lock()
			handled = append(handled, e.ID)
			handledMu.Unlock()
			startOnce.Do(func() { close(started) })
			<-release
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(100),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "first leased handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	// 槽位被第一条独占：第二条不可能已处理。
	handledMu.Lock()
	gotBefore := append([]string(nil), handled...)
	handledMu.Unlock()
	if want := []string{"ov-1"}; !slices.Equal(gotBefore, want) {
		t.Fatalf("handled before release = %v, want %v", gotBefore, want)
	}

	close(release)
	waitForUnit(t, 2*time.Second, "second over-limit lease processed after slot release", func() bool {
		handledMu.Lock()
		defer handledMu.Unlock()
		return len(handled) == 2
	})
	if want := []string{"ov-1", "ov-2"}; !slices.Equal(d.completedEventIDs(), want) {
		t.Fatalf("completed event IDs = %v, want %v", d.completedEventIDs(), want)
	}
}

// Claim 出错：记错误后轮询继续，恢复后成功认领并处理，Stop 正常返回。
func TestClaimError_Poll_ContinuesUntilSuccess(t *testing.T) {
	d := newClaimProbeDriver()
	d.failClaims(1)
	d.enqueueBatch(prefetchLease("err-1"))

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error { return nil },
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(10),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "event processed after transient claim failure", func() bool {
		return slices.Contains(d.completedEventIDs(), "err-1")
	})
	if got := d.claimCount(); got < 2 {
		t.Fatalf("claim calls = %d, want >= 2 (failure then success)", got)
	}
}

// handler 错误下推 Fail：handler 返回错误时，core 以该错误文本作为
// FailRequest.Error 下推（后端清洗后落库为终态摘要）。
func TestHandlerError_Fail_CarriesHandlerErrorText(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("pe-1"))
	handlerErr := errors.New("boom: delivery failed")

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error { return handlerErr },
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(10),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "fail recorded for failed handler", func() bool {
		return len(d.failedRequests()) == 1
	})
	if got := d.failedRequests()[0].Error; got != handlerErr.Error() {
		t.Fatalf("FailRequest.Error = %q, want %q", got, handlerErr.Error())
	}
}

// handler panic 下推 Fail：panic 转为 handlerPanicError 后同样携带文本
// 下推（认领消费 panic 计一次失败重试）。
func TestHandlerPanic_Fail_CarriesPanicErrorText(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("pp-1"))

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			panic("detonate")
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(10),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "fail recorded for panicked handler", func() bool {
		return len(d.failedRequests()) == 1
	})
	if got := d.failedRequests()[0].Error; got != "event: handler panic: detonate" {
		t.Fatalf("FailRequest.Error = %q, want handler panic text", got)
	}
}

// handler 成功不下推 Fail：只调用 Complete，不产生任何 Fail 请求。
func TestHandlerSuccess_Complete_NoFailRecorded(t *testing.T) {
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("ok-1"))

	ctx := context.Background()
	ev := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond), event.WithLogger(quietLogger()))
	if err := ev.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error { return nil },
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
		event.WithFetchLimit(10),
		event.WithLease(time.Second),
		event.WithHeartbeat(50*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "event completed", func() bool {
		return slices.Contains(d.completedEventIDs(), "ok-1")
	})
	if got := d.failedRequests(); len(got) != 0 {
		t.Fatalf("successful handler must not record Fail, got %d", len(got))
	}
}
