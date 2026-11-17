package event

import (
	"context"
	"sync"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newManualProvider 构造 ManualReader 装配的 provider（与外部测试包的
// 同名辅助一致；两测试包不共享代码）。
func newManualProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
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

func counterDatapoints(t *testing.T, r *sdkmetric.ManualReader,
	name string) []metricdata.DataPoint[int64] {
	t.Helper()
	for _, m := range collectMetrics(t, r) {
		if m.Name != name {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			t.Fatalf("metric %s data type %T, want Sum[int64]", name, m.Data)
		}
		return sum.DataPoints
	}
	t.Fatalf("metric %s not found in collected metrics", name)
	return nil
}

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

// 唤醒通道是包内私有实现（subscribe/notify），sent/dropped 口径只能在
// 内部测试直接观察。

// wakeRecordingObserver 捕获 WakeEvent（内部测试用）。
type wakeRecordingObserver struct {
	mu    sync.Mutex
	wakes []WakeEvent
}

func (o *wakeRecordingObserver) OnPublish(PublishEvent) {}
func (o *wakeRecordingObserver) OnConsume(ConsumeEvent) {}
func (o *wakeRecordingObserver) OnWake(e WakeEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.wakes = append(o.wakes, e)
}

func TestLocalWakeNotify_FullChannel_ReturnsNotifiedAndDropped(t *testing.T) {
	w := NewLocalWake()
	_ = w.subscribe("unit.wake")

	notified, dropped := w.notify("unit.wake")
	if notified != 1 || dropped != 0 {
		t.Fatalf("first notify = (%d, %d), want (1, 0)", notified, dropped)
	}
	notified, dropped = w.notify("unit.wake")
	if notified != 0 || dropped != 1 {
		t.Fatalf("second notify (channel full) = (%d, %d), want (0, 1)", notified, dropped)
	}
	notified, dropped = w.notify("unit.unknown")
	if notified != 0 || dropped != 0 {
		t.Fatalf("notify without subscriber = (%d, %d), want (0, 0)", notified, dropped)
	}
}

func TestWakeDropped_Publish_CountsSentAndDropped(t *testing.T) {
	mp, reader := newManualProvider(t)
	obs := &wakeRecordingObserver{}
	wake := NewLocalWake()
	_ = wake.subscribe("unit.wake")
	p := NewPublisher(noopDriver{}, WithMeterProvider(mp),
		WithObserver(obs), WithLocalWake(wake))
	ctx := context.Background()
	ev := Declare[NoPayload]("unit.wake")

	if err := Publish(ctx, p, ev, NoPayload{}); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := Publish(ctx, p, ev, NoPayload{}); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	dps := counterDatapoints(t, reader, "go_event.wake")
	if got := counterValueByResult(t, dps, resultSent); got != 1 {
		t.Fatalf("wake sent = %d, want 1", got)
	}
	if got := counterValueByResult(t, dps, resultDropped); got != 1 {
		t.Fatalf("wake dropped = %d, want 1", got)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.wakes) != 2 || obs.wakes[0].Notified != 1 || obs.wakes[1].Dropped != 1 {
		t.Fatalf("wake events = %+v, want notified=1 then dropped=1", obs.wakes)
	}
}

func TestPublishNoSubscribers_Wake_NotRecorded(t *testing.T) {
	mp, reader := newManualProvider(t)
	wake := NewLocalWake() // 无订阅者
	p := NewPublisher(noopDriver{}, WithMeterProvider(mp), WithLocalWake(wake))

	if err := Publish(context.Background(), p, Declare[NoPayload]("unit.wake"), NoPayload{}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, m := range collectMetrics(t, reader) {
		if m.Name == "go_event.wake" {
			t.Fatalf("wake metric recorded without subscribers, want absent")
		}
	}
}
