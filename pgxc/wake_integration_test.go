package pgxc_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/pgxc"
)

// 跨实例唤醒集成（pgxc LISTEN/NOTIFY，ADR 0013）：需真实 Postgres，
// 设置 TEST_POSTGRES_DSN 启用，未设置时跳过。信号是异步唤醒，断言以
// 有界等待收敛（沿 LocalWake 既有测试的确定性手法），不依赖 sleep 侥幸。

type wakingPayload struct {
	Value string `json:"value"`
}

// connectedAfter 捕获唤醒监听的「已连接」日志：第 1 次到达时关闭 first
// （监听就绪的确定性观察点），第 want 次到达时关闭 ch（重连恢复用）。
// 日志在 LISTEN 成功后发出。
type connectedAfter struct {
	mu          sync.Mutex
	want        int
	seen        int
	ch          chan struct{}
	closed      bool
	first       chan struct{}
	firstClosed bool
}

func newConnectedAfter(n int) *connectedAfter {
	return &connectedAfter{want: n, ch: make(chan struct{}), first: make(chan struct{})}
}

func (h *connectedAfter) logger() *slog.Logger { return slog.New(h) }

// firstConnected 返回首次连接信号通道。
func (h *connectedAfter) firstConnected() <-chan struct{} { return h.first }

func (h *connectedAfter) Enabled(context.Context, slog.Level) bool { return true }

func (h *connectedAfter) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "pgxc: wake listener connected" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen++
	if h.seen >= 1 && !h.firstClosed {
		h.firstClosed = true
		close(h.first)
	}
	if h.seen >= h.want && !h.closed {
		h.closed = true
		close(h.ch)
	}
	return nil
}

func (h *connectedAfter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *connectedAfter) WithGroup(string) slog.Handler      { return h }

// wakePool 构造订阅/发布实例的连接池；applicationName 非空时为该池全部
// 连接打标（pg_stat_activity 定位监听专用连接用）。
func wakePool(t *testing.T, applicationName string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if applicationName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = applicationName
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping db: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// startWakeAssembly 启动一个订阅「实例」：订阅侧连接池 + 跨实例唤醒总线
// （Bind 就近唤醒）+ Runner 订阅（认领或广播，轮询间隔拉长到不参与收敛
// ——唤醒必须来自跨实例信号）。阻塞至 LISTEN 首次连接建立（「已连接」
// 日志观察点）后才返回——跨实例发布依赖监听就绪，消除首发时序对构造
// 耗时的隐式竞态。返回处理计数器与第二次连接的信号通道（断连恢复用例
// 的确定性观察点；其余用例忽略）。listener 由 t.Cleanup 收尾。
func startWakeAssembly(t *testing.T, schema, applicationName string,
	broadcast bool) (func() int64, <-chan struct{}) {
	t.Helper()
	connected := newConnectedAfter(2) // 首次连接 + （恢复用例的）重连
	pool := wakePool(t, applicationName)
	wake := event.NewLocalWake()
	listener := pgxc.NewWake(pool, pgxc.WithSchema(schema), pgxc.WithLogger(connected.logger()))
	listener.Bind(wake)
	t.Cleanup(listener.Close)

	var received atomic.Int64
	drv := pgxc.NewDriver(pool, pgxc.WithSchema(schema))
	run := event.NewRunner(drv,
		event.WithPollInterval(time.Hour), // 唤醒必须来自跨实例信号
		event.WithLocalWake(wake), event.WithLogger(discardSlog()))
	ev := event.Declare[wakingPayload]("wake-cross-" + schema)
	sub := func(context.Context, event.Envelope[wakingPayload]) error {
		received.Add(1)
		return nil
	}
	var err error
	if broadcast {
		err = ev.SubscribeBroadcast(run, sub)
	} else {
		err = ev.SubscribeClaim(run, sub, event.WithConsumerType("waker"))
	}
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(t.Context()); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	t.Cleanup(func() { _ = run.Stop(context.Background()) })

	select {
	case <-connected.firstConnected():
	case <-time.After(10 * time.Second):
		t.Fatal("wake listener did not connect within 10s")
	}
	return func() int64 { return received.Load() }, connected.ch
}

// waitReceived 有界等待计数值到达 want（异步信号的确定性收敛）。
func waitReceived(t *testing.T, count func() int64, want int64, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout waiting: " + msg)
}

// wakePublisher 返回指向同一 schema 的发布实例（独立连接池 = 独立发布侧）。
func wakePublisher(t *testing.T, schema string) *event.Publisher {
	t.Helper()
	pool := wakePool(t, "")
	return event.NewPublisher(pgxc.NewDriver(pool, pgxc.WithSchema(schema)))
}

// 跨实例推送：发布实例（独立连接池）Publish 实际插入行 → NOTIFY → 订阅
// 实例收到信号立即认领（轮询间隔内不靠 poll）。信号仅触发拉取/认领，
// 读取仍走后端存储（ADR 0003 边界不变）。
func TestCrossInstanceNotify_WakesLocalRunner(t *testing.T) {
	admin := migrationPool(t)
	schema := newMigrationSchema(t, admin)
	count, _ := startWakeAssembly(t, schema, "", false) // 返回前 LISTEN 已就绪
	pub := wakePublisher(t, schema)

	if err := pub.PublishNoPayload(context.Background(),
		event.Declare[event.NoPayload]("wake-cross-"+schema),
		event.WithEventID("wake-x-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitReceived(t, count, 1, "cross-instance notify must wake the subscriber")
}

// 多实例信号：一次发布唤醒全部订阅实例。广播订阅各自独立全收（认领消费
// 单事件只被一个实例处理，信号触发的空认领轮不投递），故本用例以广播
// 断言「信号到达每一实例并触发立即拉取」。
func TestCrossInstanceNotify_MultipleInstances_AllWoken(t *testing.T) {
	admin := migrationPool(t)
	schema := newMigrationSchema(t, admin)
	count1, _ := startWakeAssembly(t, schema, "", true) // 返回前 LISTEN 已就绪
	count2, _ := startWakeAssembly(t, schema, "", true)
	pub := wakePublisher(t, schema)

	if err := pub.PublishNoPayload(context.Background(),
		event.Declare[event.NoPayload]("wake-cross-"+schema),
		event.WithEventID("wake-m-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitReceived(t, count1, 1, "instance 1 must be woken")
	waitReceived(t, count2, 1, "instance 2 must be woken")
}

// 连接断开恢复：终止 LISTEN 专用连接（模拟断连），有界退避重连并重新
// LISTEN（ADR 0013 §3）；重连窗口内的信号丢失由轮询兜底（ADR 0003），
// 恢复后的信号路径照常端到端可达。重连完成以第二次「已连接」日志为
// 确定性观察点（日志在 LISTEN 成功后发出），其后发布必达。
func TestListenerConnectionKilled_ReconnectsAndWakesAgain(t *testing.T) {
	admin := migrationPool(t)
	schema := newMigrationSchema(t, admin)
	marker := fmt.Sprintf("go_event_wake_test_%d", time.Now().UnixNano())
	count, reconnected := startWakeAssembly(t, schema, marker, false)
	pub := wakePublisher(t, schema)
	ctx := context.Background()

	// 首次发布：信号路径可用（第一次连接，装配已等待其建立）。
	if err := pub.PublishNoPayload(ctx,
		event.Declare[event.NoPayload]("wake-cross-"+schema),
		event.WithEventID("wake-r-1")); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	waitReceived(t, count, 1, "first notify must wake the subscriber")

	// 终止监听专用连接：application_name 圈定订阅实例的连接池，query
	// 前缀 `LISTEN` 精确定位监听后端（该池其余连接的最后语句非 LISTEN，
	// 误杀空闲连接不会触发重连路径）。
	var pid int
	if err := admin.QueryRow(ctx, `
SELECT pid FROM pg_stat_activity
WHERE application_name = $1 AND pid <> pg_backend_pid()
  AND query LIKE 'LISTEN go_event_wake_%'`,
		marker).Scan(&pid); err != nil {
		t.Fatalf("find wake listener connection: %v", err)
	}
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatalf("terminate wake listener connection: %v", err)
	}

	// 等待重连完成（第二次「已连接」；断连窗口内的信号本就不保证必达，
	// 断言点放在恢复完成之后）。
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("wake listener did not reconnect within 10s")
	}

	// 恢复后的发布：信号路径端到端恢复。
	if err := pub.PublishNoPayload(ctx,
		event.Declare[event.NoPayload]("wake-cross-"+schema),
		event.WithEventID("wake-r-2")); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	waitReceived(t, count, 2, "notify after reconnection must wake the subscriber")
}
