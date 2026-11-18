package event

import (
	"context"
	"errors"
	"testing"
)

// noopDriver 满足 Driver 接口：被拒绝的发布不应触达任何后端方法。
type noopDriver struct{}

func (noopDriver) Append(context.Context, RawEnvelope) error { return nil }

func (noopDriver) Fetch(context.Context, FetchQuery) ([]RawEnvelope, error) {
	return nil, nil
}

func (noopDriver) HeadPosition(context.Context, string) (string, error) { return "", nil }

func (noopDriver) Claim(context.Context, ClaimQuery) (ClaimResult, error) {
	return ClaimResult{}, nil
}

func (noopDriver) Heartbeat(context.Context, HeartbeatRequest) error { return nil }

func (noopDriver) Complete(context.Context, CompleteRequest) error { return nil }

func (noopDriver) Fail(context.Context, FailRequest) error { return nil }

// 带 LocalWake 的空串显式 ID 发布：返回 ErrEmptyEventID 且不向该类型发
// 唤醒信号（docs/api.md §2.3）。唤醒通道是包内私有实现，只能在内部测试
// 直接观察，故本用例放在 package event。
func TestEmptyEventID_Publish_NoWakeSignal(t *testing.T) {
	w := NewLocalWake()
	ch := w.subscribe("unit.emptyid.wake")
	pub := NewPublisher(noopDriver{}, WithLocalWake(w))
	ev := Declare[struct{}]("unit.emptyid.wake")

	err := Publish(context.Background(), pub, ev, struct{}{}, WithEventID(""))
	if !errors.Is(err, ErrEmptyEventID) {
		t.Fatalf("want ErrEmptyEventID, got %v", err)
	}
	select {
	case <-ch:
		t.Fatal("rejected publish must not emit wake signal")
	default:
	}
}
