package event_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// 可观测规范测试：otel 同步仪表接口密封，断言用 sdk/metric ManualReader
// 收集 metricdata（usage.md §13.4）；日志断言用自定义 slog.Handler。

// newManualProvider 构造 ManualReader 装配的 provider（快照断言用）。
func newManualProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// counterDatapoints 严格版：指标缺失即 Fatal（直接断言用）。
func counterDatapoints(t *testing.T, r *sdkmetric.ManualReader,
	name string) []metricdata.DataPoint[int64] {
	t.Helper()
	dps := counterDatapointsOrEmpty(r, name)
	if dps == nil {
		t.Fatalf("metric %s not found in collected metrics", name)
	}
	return dps
}

// counterDatapointsOrEmpty 宽容版：指标尚未被记录（未出现在收集中）返回
// nil，供轮询等待条件使用。
func counterDatapointsOrEmpty(r *sdkmetric.ManualReader,
	name string) []metricdata.DataPoint[int64] {
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		return nil
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				return nil
			}
			return sum.DataPoints
		}
	}
	return nil
}

// counterTotal 数据点值总和（不限属性）。
func counterTotal(dps []metricdata.DataPoint[int64]) int64 {
	var total int64
	for _, dp := range dps {
		total += dp.Value
	}
	return total
}

// counterValueByResult 累加 result 属性等于指定值的数据点值。
func counterValueByResult(t *testing.T, dps []metricdata.DataPoint[int64],
	result string) int64 {
	t.Helper()
	var total int64
	for _, dp := range dps {
		if v, ok := dp.Attributes.Value("result"); ok && v.AsString() == result {
			total += dp.Value
		}
	}
	return total
}

// histogramCountOrZero 返回直方图记录次数；指标不存在（未记录过）返回 0。
func histogramCountOrZero(t *testing.T, r *sdkmetric.ManualReader, name string) uint64 {
	t.Helper()
	for _, m := range collectMetrics(t, r) {
		if m.Name != name {
			continue
		}
		h, ok := m.Data.(metricdata.Histogram[float64])
		if !ok {
			t.Fatalf("metric %s data type %T, want Histogram[float64]", name, m.Data)
		}
		var count uint64
		for _, dp := range h.DataPoints {
			count += dp.Count
		}
		return count
	}
	return 0
}

func collectMetrics(t *testing.T, r *sdkmetric.ManualReader) []metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var out []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		out = append(out, sm.Metrics...)
	}
	return out
}

// recordingObserver 捕获全部观测事件（实现完整 ObserverV4，含 V3 的
// 管理面事件——V4 经嵌入链要求全部方法）。
type recordingObserver struct {
	mu     sync.Mutex
	pubs   []event.PublishEvent
	cons   []event.ConsumeEvent
	wakes  []event.WakeEvent
	leases []event.LeaseEvent
	stops  []event.StopEvent
	replay []event.AdminReplayEvent
	claims []event.ClaimEvent
}

func (o *recordingObserver) OnPublish(e event.PublishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pubs = append(o.pubs, e)
}

func (o *recordingObserver) OnConsume(e event.ConsumeEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cons = append(o.cons, e)
}

func (o *recordingObserver) OnWake(e event.WakeEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.wakes = append(o.wakes, e)
}

func (o *recordingObserver) OnLeaseAbandoned(e event.LeaseEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.leases = append(o.leases, e)
}

func (o *recordingObserver) OnRunnerStopped(e event.StopEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stops = append(o.stops, e)
}

func (o *recordingObserver) OnAdminReplay(e event.AdminReplayEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.replay = append(o.replay, e)
}

func (o *recordingObserver) OnClaim(e event.ClaimEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claims = append(o.claims, e)
}

func (o *recordingObserver) snapshot() (pubs []event.PublishEvent,
	cons []event.ConsumeEvent, wakes []event.WakeEvent,
	leases []event.LeaseEvent, stops []event.StopEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event.PublishEvent(nil), o.pubs...),
		append([]event.ConsumeEvent(nil), o.cons...),
		append([]event.WakeEvent(nil), o.wakes...),
		append([]event.LeaseEvent(nil), o.leases...),
		append([]event.StopEvent(nil), o.stops...)
}

// observerPanic 回调恒 panic 的观察者：旁路不得击穿调用方。
type observerPanic struct{}

func (observerPanic) OnPublish(event.PublishEvent) { panic("boom") }
func (observerPanic) OnConsume(event.ConsumeEvent) { panic("boom") }
func (observerPanic) OnWake(event.WakeEvent)       { panic("boom") }

// failCodec 编码恒失败的 codec（encode 错误路径注入）。
type failCodec struct{}

func (failCodec) Encode(any) ([]byte, error) { return nil, errors.New("codec down") }
func (failCodec) Decode([]byte, any) error   { return errors.New("codec down") }

// heartbeatFailDriver 包装任意 Driver 实现：Heartbeat 注入错误。
type heartbeatFailDriver struct {
	event.Driver
	err error
}

func (d *heartbeatFailDriver) Heartbeat(context.Context, event.HeartbeatRequest) error {
	return d.err
}

var obsType = event.Declare[runPayload]("unit.obs")

func TestPublishResult_Publish_CountsResultAndDuration(t *testing.T) {
	cases := []struct {
		name     string
		result   string
		build    func(t *testing.T, d *stubDriver, mp *sdkmetric.MeterProvider) (*event.Publisher, []event.PublishOption)
		duration uint64 // 预期 duration 记录次数
		wantErr  bool
	}{
		{
			name:     "AppendOK_Publish_CountsOkWithDuration",
			result:   "ok",
			duration: 1,
			build: func(t *testing.T, d *stubDriver, mp *sdkmetric.MeterProvider) (*event.Publisher, []event.PublishOption) {
				return event.NewPublisher(d, event.WithMeterProvider(mp)), nil
			},
		},
		{
			name:    "EmptyEventID_Publish_CountsInvalidIDWithoutDuration",
			result:  "invalid_id",
			wantErr: true,
			build: func(t *testing.T, d *stubDriver, mp *sdkmetric.MeterProvider) (*event.Publisher, []event.PublishOption) {
				return event.NewPublisher(d, event.WithMeterProvider(mp)),
					[]event.PublishOption{event.WithEventID("")}
			},
		},
		{
			name:    "EncodeFailure_Publish_CountsEncodeErrorWithoutDuration",
			result:  "encode_error",
			wantErr: true,
			build: func(t *testing.T, d *stubDriver, mp *sdkmetric.MeterProvider) (*event.Publisher, []event.PublishOption) {
				return event.NewPublisher(d, event.WithMeterProvider(mp),
					event.WithPublisherCodec(failCodec{})), nil
			},
		},
		{
			name:     "AppendFailure_Publish_CountsAppendErrorWithDuration",
			result:   "append_error",
			duration: 1,
			wantErr:  true,
			build: func(t *testing.T, d *stubDriver, mp *sdkmetric.MeterProvider) (*event.Publisher, []event.PublishOption) {
				d.mu.Lock()
				d.appendErr = errStub
				d.mu.Unlock()
				return event.NewPublisher(d, event.WithMeterProvider(mp)), nil
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newManualProvider(t)
			p, opts := tt.build(t, newStubDriver(), mp)

			err := event.Publish(context.Background(), p, obsType, runPayload{Value: "x"}, opts...)
			if tt.wantErr && err == nil {
				t.Fatalf("publish = nil, want error for result %s", tt.result)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("publish = %v, want nil", err)
			}

			dps := counterDatapoints(t, reader, "go_event.publish")
			if got := counterValueByResult(t, dps, tt.result); got != 1 {
				t.Fatalf("publish result=%s count = %d, want 1", tt.result, got)
			}
			for _, dp := range dps {
				if v, ok := dp.Attributes.Value("event"); !ok || v.AsString() != obsType.Name {
					t.Fatalf("publish datapoint attrs = %v, want event=%q", dp.Attributes, obsType.Name)
				}
			}
			if got := histogramCountOrZero(t, reader, "go_event.publish.duration"); got != tt.duration {
				t.Fatalf("publish duration count = %d, want %d", got, tt.duration)
			}
		})
	}
}

func TestObserverPanic_Publish_BypassedAndCounted(t *testing.T) {
	mp, reader := newManualProvider(t)
	p := event.NewPublisher(newStubDriver(), event.WithMeterProvider(mp),
		event.WithObserver(observerPanic{}))
	if err := event.Publish(context.Background(), p, obsType, runPayload{Value: "x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := counterValueByResult(t, counterDatapoints(t, reader, "go_event.publish"), "ok"); got != 1 {
		t.Fatalf("publish ok count = %d, want 1 (observer panic must be bypassed)", got)
	}
}

func TestObserverV2_PublishAndConsume_EventsCarryContractFields(t *testing.T) {
	mp, _ := newManualProvider(t)
	obs := &recordingObserver{}
	wake := event.NewLocalWake()
	d := newStubDriver()
	p := event.NewPublisher(d, event.WithMeterProvider(mp),
		event.WithObserver(obs), event.WithLocalWake(wake))
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(quietLogger()), event.WithObserver(obs),
		event.WithMeterProvider(mp), event.WithLocalWake(wake))
	ctx := context.Background()
	if err := obsType.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()
	if err := event.Publish(ctx, p, obsType, runPayload{Value: "x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitForUnit(t, 2*time.Second, "consume ok event observed", func() bool {
		_, cons, _, _, _ := obs.snapshot()
		for _, e := range cons {
			if e.Result == "ok" {
				return true
			}
		}
		return false
	})
	pubs, cons, wakes, _, _ := obs.snapshot()
	if len(pubs) == 0 || pubs[0].Result != "ok" || pubs[0].Event != obsType.Name {
		t.Fatalf("publish events = %+v, want ok with event %q", pubs, obsType.Name)
	}
	found := false
	for _, e := range cons {
		if e.Result == "ok" {
			found = true
			if e.Mode != "broadcast" || e.Event != obsType.Name {
				t.Fatalf("consume event = %+v, want mode=broadcast event=%q", e, obsType.Name)
			}
		}
	}
	if !found {
		t.Fatalf("consume ok event missing: %+v", cons)
	}
	if len(wakes) == 0 || wakes[0].Notified < 1 {
		t.Fatalf("wake events = %+v, want notified >= 1", wakes)
	}
}

func TestFetchFailure_Poll_CountsError(t *testing.T) {
	mp, reader := newManualProvider(t)
	d := newStubDriver()
	d.mu.Lock()
	d.fetchErrs = 1
	d.mu.Unlock()
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(quietLogger()), event.WithMeterProvider(mp))
	ctx := context.Background()
	if err := obsType.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "fetch error datapoint", func() bool {
		return counterValueByResult(t, counterDatapointsOrEmpty(reader, "go_event.fetch"), "error") >= 1
	})
	if got := histogramCountOrZero(t, reader, "go_event.fetch.duration"); got < 1 {
		t.Fatalf("fetch duration count = %d, want >= 1", got)
	}
}

func TestClaimHandlerError_Fail_CountsHandlerErrorAndFailOK(t *testing.T) {
	mp, reader := newManualProvider(t)
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("cl-err"))
	claimEv := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(quietLogger()), event.WithMeterProvider(mp))
	ctx := context.Background()
	if err := claimEv.SubscribeClaim(run, func(context.Context, event.Envelope[runPayload]) error {
		return errStub
	}, event.WithConsumerType(prefetchConsumer)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	waitForUnit(t, 2*time.Second, "fail ok datapoint", func() bool {
		return counterValueByResult(t, counterDatapointsOrEmpty(reader, "go_event.fail"), "ok") >= 1
	})
	if got := counterValueByResult(t, counterDatapoints(t, reader, "go_event.consume"), "handler_error"); got < 1 {
		t.Fatalf("consume handler_error = %d, want >= 1", got)
	}
	if got := counterTotal(counterDatapoints(t, reader, "go_event.claim.granted")); got < 1 {
		t.Fatalf("claim.granted total = %d, want >= 1", got)
	}
}

func TestBroadcastHandlerPanic_Consume_CountsHandlerPanic(t *testing.T) {
	mp, reader := newManualProvider(t)
	d := newStubDriver()
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(quietLogger()), event.WithMeterProvider(mp))
	ctx := context.Background()
	if err := obsType.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		panic("handler exploded")
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()
	if err := event.Publish(ctx, event.NewPublisher(d), obsType, runPayload{Value: "x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitForUnit(t, 2*time.Second, "handler_panic datapoint", func() bool {
		return counterValueByResult(t, counterDatapointsOrEmpty(reader, "go_event.consume"), "handler_panic") >= 1
	})
}

func TestHeartbeatFailure_Heartbeat_CountsError(t *testing.T) {
	mp, reader := newManualProvider(t)
	d := &heartbeatFailDriver{Driver: newClaimProbeDriver(), err: errStub}
	d.Driver.(*claimProbeDriver).enqueueBatch(prefetchLease("hb-1"))
	claimEv := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(quietLogger()), event.WithMeterProvider(mp))
	ctx := context.Background()
	release := make(chan struct{})
	if err := claimEv.SubscribeClaim(run, func(context.Context, event.Envelope[runPayload]) error {
		<-release // 挂住 handler 使心跳得以发生
		return nil
	}, event.WithConsumerType(prefetchConsumer),
		event.WithLease(90*time.Millisecond),
		event.WithHeartbeat(30*time.Millisecond),
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

	waitForUnit(t, 2*time.Second, "heartbeat error datapoint", func() bool {
		return counterValueByResult(t, counterDatapointsOrEmpty(reader, "go_event.heartbeat"), "error") >= 1
	})
}

func TestRunnerLifecycle_StartStop_LogsAndObserver(t *testing.T) {
	mp, _ := newManualProvider(t)
	watcher := newLogWatcher()
	obs := &recordingObserver{}
	run := event.NewRunner(newStubDriver(), event.WithPollInterval(time.Millisecond),
		event.WithLogger(slog.New(watcher)), event.WithObserver(obs), event.WithMeterProvider(mp))
	ctx := context.Background()
	if err := obsType.SubscribeBroadcast(run, func(context.Context, event.Envelope[runPayload]) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	watcher.waitFor(t, "event: runner started")
	if err := run.Stop(context.WithoutCancel(ctx)); err != nil {
		t.Fatalf("stop: %v", err)
	}
	watcher.waitFor(t, "event: runner stopped")
	_, _, _, _, stops := obs.snapshot()
	if len(stops) != 1 || stops[0].TimedOut {
		t.Fatalf("stop events = %+v, want exactly one TimedOut=false", stops)
	}
}

func TestStopTimedOut_Stop_ReturnsCtxErrAndObserverTimedOut(t *testing.T) {
	mp, _ := newManualProvider(t)
	watcher := newLogWatcher()
	obs := &recordingObserver{}
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("stop-to-1"))
	claimEv := event.Declare[runPayload](prefetchType)
	run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
		event.WithLogger(slog.New(watcher)), event.WithObserver(obs), event.WithMeterProvider(mp))
	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})
	if err := claimEv.SubscribeClaim(run, func(context.Context, event.Envelope[runPayload]) error {
		close(started)
		<-release
		return nil
	}, event.WithConsumerType(prefetchConsumer)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		close(release)
		_ = run.Stop(context.WithoutCancel(ctx))
	}()

	waitForUnit(t, 2*time.Second, "handler started", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := run.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop = %v, want DeadlineExceeded", err)
	}
	watcher.waitFor(t, "event: runner stop timed out, in-flight handlers continuing")
	_, _, _, _, stops := obs.snapshot()
	if len(stops) != 1 || !stops[0].TimedOut {
		t.Fatalf("stop events = %+v, want exactly one TimedOut=true", stops)
	}
}

// 接管与终态计数随 ClaimResult 计量：claim.takeover / claim.terminal_failed
// 与落库事实一致（design.md §9.2、ADR 0008），ObserverV4 收到同口径事件；
// 无接管的发放轮次不产生接管数据点。
func TestClaimTakeover_Claim_CountsTakeoverAndTerminalFailed(t *testing.T) {
	t.Run("TakeoverAndTerminal_CountedWithIdentityAttrs", func(t *testing.T) {
		mp, reader := newManualProvider(t)
		obs := &recordingObserver{}
		d := newClaimProbeDriver()
		d.enqueueResult(event.ClaimResult{
			Leases:         []event.Lease{prefetchLease("to-1")},
			Takeovers:      1,
			TerminalFailed: 1,
		})
		claimEv := event.Declare[runPayload](prefetchType)
		run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
			event.WithLogger(quietLogger()), event.WithObserver(obs), event.WithMeterProvider(mp))
		ctx := context.Background()
		if err := claimEv.SubscribeClaim(run, func(context.Context, event.Envelope[runPayload]) error {
			return nil
		}, event.WithConsumerType(prefetchConsumer)); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := run.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

		waitForUnit(t, 2*time.Second, "claim.takeover datapoint", func() bool {
			return counterTotal(counterDatapointsOrEmpty(reader, "go_event.claim.takeover")) >= 1
		})
		takeoverDps := counterDatapoints(t, reader, "go_event.claim.takeover")
		if got := counterTotal(takeoverDps); got != 1 {
			t.Fatalf("claim.takeover total = %d, want 1", got)
		}
		for _, dp := range takeoverDps {
			if v, ok := dp.Attributes.Value("event"); !ok || v.AsString() != prefetchType {
				t.Fatalf("claim.takeover attrs = %v, want event=%q", dp.Attributes, prefetchType)
			}
			if v, ok := dp.Attributes.Value("identity"); !ok || v.AsString() != prefetchConsumer {
				t.Fatalf("claim.takeover attrs = %v, want identity=%q", dp.Attributes, prefetchConsumer)
			}
		}
		if got := counterTotal(counterDatapoints(t, reader, "go_event.claim.terminal_failed")); got != 1 {
			t.Fatalf("claim.terminal_failed total = %d, want 1", got)
		}
		waitForUnit(t, 2*time.Second, "observer claim event", func() bool {
			obs.mu.Lock()
			defer obs.mu.Unlock()
			return len(obs.claims) >= 1
		})
		obs.mu.Lock()
		defer obs.mu.Unlock()
		var found bool
		for _, e := range obs.claims {
			if e.Takeovers == 1 && e.TerminalFailed == 1 && e.Leases == 1 &&
				e.Event == prefetchType && e.Identity == prefetchConsumer {
				found = true
			}
		}
		if !found {
			t.Fatalf("observer claim events = %+v, want takeover=1 terminal_failed=1 leases=1", obs.claims)
		}
	})

	t.Run("FreshGrantOnly_Claim_NoTakeoverDatapoint", func(t *testing.T) {
		mp, reader := newManualProvider(t)
		d := newClaimProbeDriver()
		d.enqueueBatch(prefetchLease("to-2"))
		claimEv := event.Declare[runPayload](prefetchType)
		run := event.NewRunner(d, event.WithPollInterval(time.Millisecond),
			event.WithLogger(quietLogger()), event.WithMeterProvider(mp))
		ctx := context.Background()
		if err := claimEv.SubscribeClaim(run, func(context.Context, event.Envelope[runPayload]) error {
			return nil
		}, event.WithConsumerType(prefetchConsumer)); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := run.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

		waitForUnit(t, 2*time.Second, "claim.granted datapoint", func() bool {
			return counterTotal(counterDatapointsOrEmpty(reader, "go_event.claim.granted")) >= 1
		})
		for _, m := range collectMetrics(t, reader) {
			if m.Name == "go_event.claim.takeover" || m.Name == "go_event.claim.terminal_failed" {
				t.Fatalf("fresh grant must not record %s", m.Name)
			}
		}
	})
}

func TestAbandonedLease_OnStop_CountsMetricAndObserverEvent(t *testing.T) {
	mp, reader := newManualProvider(t)
	obs := &recordingObserver{}
	watcher := newLogWatcher()
	d := newClaimProbeDriver()
	d.enqueueBatch(prefetchLease("obs-stop-1"), prefetchLease("obs-stop-2")) // 2 > Limit 1

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	stopRelease := func() { releaseOnce.Do(func() { close(release) }) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := event.NewRunner(d, event.WithPollInterval(2*time.Millisecond),
		event.WithLogger(slog.New(watcher)), event.WithObserver(obs), event.WithMeterProvider(mp))
	if err := obsType.SubscribeClaim(run,
		func(context.Context, event.Envelope[runPayload]) error {
			startOnce.Do(func() { close(started) })
			<-release
			return nil
		},
		event.WithConsumerType(prefetchConsumer),
		event.WithConcurrency(1),
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
	cancel()
	watcher.waitFor(t, "event: stopping, lease released for takeover")

	waitForUnit(t, 2*time.Second, "lease abandoned datapoint", func() bool {
		return counterTotal(counterDatapointsOrEmpty(reader, "go_event.lease.abandoned")) >= 1
	})
	_, _, _, leases, _ := obs.snapshot()
	if len(leases) == 0 || leases[0].Event != obsType.Name {
		t.Fatalf("lease events = %+v, want event %q", leases, obsType.Name)
	}
}
