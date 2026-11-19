package pgxc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// 跨实例唤醒（pgxc 原生推送，ADR 0013）：仅信号、不承载投递保证——信号
// 触发本实例已挂起的 Fetch / 唤醒等待立即拉取/认领，读取与认领仍走后端
// 存储（ADR 0003 边界不变）；错过的信号（断连窗口、通道满、本实例无对应
// 订阅）由周期 poll 兜底。
const (
	wakeInitialBackoff = 100 * time.Millisecond
	wakeMaxBackoff     = 30 * time.Second
)

// Wake 跨实例唤醒总线：专用连接 LISTEN go_event_wake_<schema> 通道。同
// schema 任一实例经 Append / AppendTx 实际插入事件行即 NOTIFY（通道与
// payload 只带 schema 名，不带事件内容，ADR 0013 §2）；本实例收到通知后
// 向 Bind 注入的 LocalWake 注入 NotifyAll 信号（跨实例信号触发本地已挂起
// 的 Fetch/唤醒等待）。
//
// 连接管理（ADR 0013 §3）：LISTEN 需专用连接——经连接池 Acquire 持有一条
// 连接、不随 DML 归还复用（LISTEN 会话状态无法随池连接归还清空）；连接
// 断开按有界退避重连并重新 LISTEN，重连窗口内的信号丢失由轮询兜底；延迟
// 行的 NOTIFY 在插入（事务提交）时发出、不判断 deliver_at（推送只回答
// 「有新行」，空拉由轮询兜底）。构造即后台监听；Close 幂等释放并停止重连。
type Wake struct {
	pool    *pgxpool.Pool
	channel string
	logger  *slog.Logger

	mu     sync.Mutex
	bridge *event.LocalWake // Bind 注入；nil = 未桥接（信号丢弃）

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// NewWake 构造跨实例唤醒总线并开始后台监听。schema 名在构造期校验：
// 非法 schema 或连接池为 nil 时监听不启动（构造仍返回可用对象，跨实例
// 信号退化由轮询兜底，ADR 0013 §3），仅记错误日志。任意合法 schema 都有
// 唤醒通道（超长 schema 走确定性哈希通道名，见 wakeChannel）。选项与
// NewDriver 共享（本面消费 schema 与 logger）。
func NewWake(pool *pgxpool.Pool, opts ...Option) *Wake {
	cfg := newConfig(opts)
	w := &Wake{
		pool:    pool,
		channel: wakeChannel(cfg.schema),
		logger:  cfg.logger,
	}
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		w.logger.Error("pgxc: wake: invalid schema, listener not started",
			"error", err.Error())
		return w
	}
	if pool == nil {
		w.logger.Error("pgxc: wake: nil pool, listener not started")
		return w
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wg.Add(1)
	go w.listenLoop()
	return w
}

// Bind 桥接本实例就近唤醒总线：通知到达即向 lw 注入 NotifyAll 信号。
// 可重复调用（后一次生效）；未桥接时信号静默丢弃（轮询兜底）。
func (w *Wake) Bind(lw *event.LocalWake) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bridge = lw
}

// Close 停止监听并释放专用连接（幂等）：阻塞至监听循环退出。未桥接或
// 监听未启动时为无操作。
func (w *Wake) Close() {
	w.closeOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
		w.wg.Wait()
	})
}

// dispatch 处理一条通知：向桥接的 LocalWake 发 NotifyAll（仅信号，
// ADR 0013 §2）。payload 只带 schema 名、不参与路由（通道本身按 schema
// 隔离），异常 payload 静默忽略。
func (w *Wake) dispatch(payload string) {
	w.mu.Lock()
	bridge := w.bridge
	w.mu.Unlock()
	if bridge == nil {
		return
	}
	bridge.NotifyAll()
}

// listenLoop 监听主循环：断开按有界退避重连（成功建立监听后重置），
// Close 取消退出。重连窗口内的信号丢失由轮询兜底（ADR 0003/0013）。
func (w *Wake) listenLoop() {
	defer w.wg.Done()
	backoff := wakeInitialBackoff
	connectedOnce := false
	for {
		listened, err := w.listenOnce()
		if w.ctx.Err() != nil {
			return // Close：不再重连
		}
		if listened {
			backoff = wakeInitialBackoff
		}
		// 措辞区分（日志即运维入口）：本次尝试内建立过监听后断开才是
		// disconnected；从未连接成功（首连失败）与曾连接后的重连尝试
		// 失败分列，避免「从未连接却报断开」的误导。
		switch {
		case listened:
			connectedOnce = true
			w.logger.Warn("pgxc: wake listener disconnected, reconnecting",
				"error", err.Error(), "backoff", backoff.String())
		case connectedOnce:
			w.logger.Warn("pgxc: wake listener reconnect failed, retrying",
				"error", err.Error(), "backoff", backoff.String())
		default:
			w.logger.Warn("pgxc: wake listener connect failed, retrying",
				"error", err.Error(), "backoff", backoff.String())
		}
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > wakeMaxBackoff {
			backoff = wakeMaxBackoff
		}
	}
}

// listenOnce 取一条专用连接 LISTEN 并阻塞等待通知，直到连接断开或 Close。
// listened 表示监听是否成功建立（供重连退避重置）。
func (w *Wake) listenOnce() (listened bool, err error) {
	conn, err := w.pool.Acquire(w.ctx)
	if err != nil {
		return false, fmt.Errorf("pgxc: wake acquire: %w", err)
	}
	// 通道名由构造期校验的 schema 派生（标识符字符 + 固定前缀），可安全内插。
	if _, err = conn.Exec(w.ctx, "LISTEN "+w.channel); err != nil {
		conn.Release()
		return false, fmt.Errorf("pgxc: wake listen: %w", err)
	}
	w.logger.Info("pgxc: wake listener connected", "channel", w.channel)
	for {
		n, err := conn.Conn().WaitForNotification(w.ctx)
		if err != nil {
			conn.Release()
			return true, fmt.Errorf("pgxc: wake wait: %w", err)
		}
		w.dispatch(n.Payload)
	}
}
