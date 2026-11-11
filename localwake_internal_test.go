package event

import (
	"sync"
	"testing"
)

// notify 扇出：同类型的全部订阅者各收到一次信号。
func TestMultipleSubscribers_Notify_EachReceivesOnce(t *testing.T) {
	w := NewLocalWake()
	ch1 := w.subscribe("t")
	ch2 := w.subscribe("t")

	w.notify("t")

	for i, ch := range []chan struct{}{ch1, ch2} {
		select {
		case <-ch:
		default:
			t.Fatalf("subscriber %d must receive signal", i+1)
		}
	}
}

// 信号容量 1：连续多次 notify 合并为一次待收信号，不阻塞发布方。
func TestRepeatedNotify_Merges_ToSingleSignal(t *testing.T) {
	w := NewLocalWake()
	ch := w.subscribe("t")

	w.notify("t")
	w.notify("t") // 容量 1：非阻塞丢弃

	select {
	case <-ch:
	default:
		t.Fatal("at least one signal must be buffered")
	}
	select {
	case <-ch:
		t.Fatal("second signal must have been merged away")
	default:
	}
}

// unsubscribe 后不再接收该订阅者的信号。
func TestUnsubscribe_Notify_NotDelivered(t *testing.T) {
	w := NewLocalWake()
	ch := w.subscribe("t")
	w.unsubscribe("t", ch)

	w.notify("t")

	select {
	case <-ch:
		t.Fatal("unsubscribed channel must not receive signals")
	default:
	}
}

// notify 对无订阅者的类型是空操作，并发安全。
func TestNotifyWithoutSubscribers_IsNoop(t *testing.T) {
	w := NewLocalWake()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				w.notify("nobody")
			}
		})
	}
	wg.Wait()
}

// 不同类型的信号互不串扰。
func TestNotifyOtherType_Isolated(t *testing.T) {
	w := NewLocalWake()
	ch := w.subscribe("a")

	w.notify("b")

	select {
	case <-ch:
		t.Fatal("signal for other type must not reach this subscriber")
	default:
	}
}
