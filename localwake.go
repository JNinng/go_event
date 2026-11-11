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

// notify 向订阅某事件类型的本实例 poller 发唤醒信号（非阻塞、可丢弃；
// Publisher 内部使用）。并发安全。
func (w *LocalWake) notify(typ string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.subs[typ] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
