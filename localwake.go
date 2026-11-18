package event

import "sync"

// LocalWake 同进程唤醒总线：发布侧发信号、Runner 按订阅注册表接收。
// 仅信号，不是投递（ADR 0003）；同一 LocalWake 可同时注入 Publisher
// 与多个 Runner。信号不承载投递保证，跨实例与丢失由周期 poll 兜底。
type LocalWake struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

// NewLocalWake 构造同进程唤醒总线。
func NewLocalWake() *LocalWake {
	return &LocalWake{subs: make(map[string]map[chan struct{}]struct{})}
}

// subscribe 注册对某事件类型的唤醒接收（Runner 内部使用）。
func (w *LocalWake) subscribe(typ string) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan struct{}, 1)
	m, ok := w.subs[typ]
	if !ok {
		m = make(map[chan struct{}]struct{})
		w.subs[typ] = m
	}
	m[ch] = struct{}{}
	return ch
}

// unsubscribe 移除唤醒接收（Runner 内部使用）。
func (w *LocalWake) unsubscribe(typ string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if m, ok := w.subs[typ]; ok {
		delete(m, ch)
		if len(m) == 0 {
			delete(w.subs, typ)
		}
	}
}

// notify 向订阅某事件类型的本实例 poller 发唤醒信号（非阻塞；Publisher
// 内部使用）。返回送达与因通道满被丢弃的订阅通道数（无订阅者返回 0, 0，
// 丢弃由周期 poll 兜底，ADR 0003）。并发安全。
func (w *LocalWake) notify(typ string) (notified, dropped int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.subs[typ] {
		select {
		case ch <- struct{}{}:
			notified++
		default:
			dropped++
		}
	}
	return notified, dropped
}

// NotifyAll 向本实例全部已订阅类型发唤醒信号（非阻塞；跨实例唤醒的桥接
// 入口，ADR 0013）：支持原生推送的后端（pgxc LISTEN/NOTIFY）收到通知后
// 经此注入信号，语义与发布侧 notify 一致——仅信号、不承载投递保证，丢弃
// 与错过的信号由周期 poll 兜底（ADR 0003 边界不变）。返回送达与丢弃的
// 订阅通道数。并发安全。
func (w *LocalWake) NotifyAll() (notified, dropped int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range w.subs {
		for ch := range m {
			select {
			case ch <- struct{}{}:
				notified++
			default:
				dropped++
			}
		}
	}
	return notified, dropped
}
