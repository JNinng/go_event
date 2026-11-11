# go\_event API 签名

- 状态：**已定稿（实施基线）**，2026-11-11
- 能力面与语义的单一事实源是 [design.md](design.md) 与 [adr/](adr/)；本文
  是签名定稿载体，与两者冲突时以语义文档为准并回改本文。

## 1. 包布局与类型形态

```
github.com/jninng/go_event           # core：契约与编排（package event）
github.com/jninng/go_event/postgrec  # Postgres 后端，标准库 database/sql（package postgrec）
github.com/jninng/go_event/pgxc      # Postgres 后端，pgx v5（package pgxc）
github.com/jninng/go_event/eventtest # 契约测试套件（package eventtest / eventtest/inmem）
```

- **接口面只有 `Driver` 一个**（core 是它的消费方，接口定义在 core）。
  Go 接口不支持泛型方法，强类型入口落在具体类型上：
  `Publisher`（发布实现）与 `Runner`（消费编排）为具体类型，`Event[T]`
  是**无状态声明值**，承载强类型糖方法，依赖经参数显式传入。
- **消费方式订阅级声明**：`SubscribeBroadcast` / `SubscribeClaim` 命名式
  入口，同一个 Runner 可混合承载两种方式的订阅（与 ADR 0002 两方式
  正交一致）。
- 无包级可变状态、无 init 魔法，全部依赖经构造函数显式注入。

## 2. core 签名

### 2.1 事件声明

```go
package event

// NoPayload 无参事件的载荷占位类型（design.md §4）。
type NoPayload struct{}

// Event[T] 事件声明值。Name 是认领与路由的稳定标识，改名即破坏性变更
// （design.md §4）。值为纯声明，不携带运行时状态；Publish / Subscribe*
// 为强类型糖，把依赖（Publisher / Runner）经参数显式传入。
type Event[T any] struct {
    Name string
}

// Declare 声明强类型事件；无参事件即 Declare[NoPayload](name)。
func Declare[T any](name string) Event[T]

// ---- 事件侧强类型入口（糖，委托给 Publisher / Runner 的实现） ----

// Publish 发布事件（对称糖，等价 p.Publish(ctx, ev, payload, opts...)）。
func (ev Event[T]) Publish(ctx context.Context, p *Publisher,
    payload T, opts ...PublishOption) error

// SubscribeBroadcast 注册广播订阅：发完即忘（ADR 0006）。仅接受
// WithConcurrency / WithFetchLimit；传入认领专属选项返回
// ErrNotClaimSubscription。
func (ev Event[T]) SubscribeBroadcast(r *Runner, h Handler[T],
    opts ...SubscribeOption) error

// SubscribeClaim 注册认领订阅（ADR 0006）。身份取 WithConsumerType，
// 未设置时用 Runner 构造默认身份（WithIdentity）；两者皆缺返回
// ErrIdentityMissing。
func (ev Event[T]) SubscribeClaim(r *Runner, h Handler[T],
    opts ...SubscribeOption) error
```

订阅须在 `Runner.Start` 前完成；Start 后调用返回 `ErrRunnerStarted`
（design.md §5「订阅在 Runner 启动前注册完毕」）。

### 2.2 信封与编解码

```go
// Envelope 面向使用方的事件信封（强类型 payload）。
type Envelope[T any] struct {
    ID         string            // 发布侧生成的唯一 ID；幂等去重与追踪主键
    Type       string            // 即 Event.Name
    Payload    T
    Metadata   map[string]string // 透传元数据，如 trace_id（design.md §4）
    OccurredAt time.Time
}

// RawEnvelope 面向 Driver 的事件信封（payload 已编码）。后端必须原样
// 落库透传（design.md §4）。
type RawEnvelope struct {
    ID         string
    Type       string
    Payload    []byte
    Metadata   map[string]string
    OccurredAt time.Time
    // Position 是后端定义的全序位置（如 Postgres bigserial），仅用于
    // 广播消费的已读水位条件（ADR 0004）：core 内存按订阅类型维护、
    // 只进不退，Fetch 时原样回传。
    Position string
}

// Codec payload 编解码，缺省 JSON，可插拔（design.md §6）。
type Codec interface {
    Encode(payload any) ([]byte, error)
    Decode(data []byte, into any) error
}

func NewJSONCodec() *JSONCodec
```

### 2.3 Publisher

```go
type IDGenerator func() string

// NewPublisher 构造生产者。依赖注入：Driver、ID 生成器（缺省内置实现）、
// codec（缺省 JSON）、就近唤醒总线（可选）。
func NewPublisher(d Driver, opts ...PublisherOption) *Publisher

func WithPublisherCodec(c Codec) PublisherOption
func WithIDGenerator(g IDGenerator) PublisherOption
// WithLocalWake(w *LocalWake)  // 发布后发唤醒信号（ADR 0003）；
                                // 单一实现同时实现 PublisherOption 与
                                // RunnerOption，可传给两侧构造器

// Publish 发布实现：ID 与 OccurredAt 缺省自动填充（design.md §6）。
// 事件侧糖 Event[T].Publish 委托到此。因 Go 方法不支持类型参数，
// Publish 为包级泛型函数而非 Publisher 方法。
func Publish[T any](ctx context.Context, p *Publisher, ev Event[T],
    payload T, opts ...PublishOption) error

// PublishNoPayload 无参事件糖，等价 Publish(ctx, ev, NoPayload{})。
func (p *Publisher) PublishNoPayload(ctx context.Context, ev Event[NoPayload],
    opts ...PublishOption) error

func WithMetadata(meta map[string]string) PublishOption // 合并进缺省元数据
```

### 2.4 Driver（唯一接口，最小缝合点）

```go
// Driver 单次原子操作（design.md §4、ADR 0004）：全部编排逻辑在 core 的
// Runner 完成。所有方法语义由 eventtest 契约测试约束。
type Driver interface {
    // Append 幂等：重复 Append 同 ID 视为成功（首写为准，不覆盖既有行、
    // 不触发重投，不校验 payload 一致性）。
    Append(ctx context.Context, e RawEnvelope) error

    // Fetch 广播消费读取：Type 匹配且 Position 严格大于 q.After 的行，
    // 按 Position 升序返回至多 q.Limit 条。只读，不产生任何簿记。
    // q.After 为空串表示从头；非空但无法解析为 Position 时返回错误。
    Fetch(ctx context.Context, q FetchQuery) ([]RawEnvelope, error)

    // HeadPosition 广播基线锚点：返回该类型当前已提交事件的最大
    // Position，无事件返回空串。core 在 Runner.Start 时对每个广播
    // 订阅调用一次，水位锚定到该快照，Start 返回即基线确立（ADR 0006）。
    HeadPosition(ctx context.Context, typ string) (string, error)

    // Claim 认领消费：原子认领至多 q.Limit 条「无主、租约过期、或退避
    // 期满」的行（Postgres 实现为 FOR UPDATE SKIP LOCKED）。已 done /
    // failed 的行不可认领；接管（租约过期）认领递增 claim_count
    // （ADR 0006）；达 q.RetryLimit 的接管认领置 failed 终态、不再认领
    // （终态判定需认领侧获知上限，故由 core 随条件下推）。批次内顺序
    // 不承诺（契约仅约束 Fetch 升序）。
    Claim(ctx context.Context, q ClaimQuery) ([]Lease, error)

    // Heartbeat 续租：仅当属主凭证匹配时生效；租约过期或已被接管后
    // 迟到的心跳不生效（CAS 不命中静默返回 nil）。
    Heartbeat(ctx context.Context, h HeartbeatRequest) error

    // Complete 置 done：幂等、不可逆；仅当属主凭证匹配时生效（CAS），
    // 迟到的 Complete 不生效、不覆盖新属主状态（ADR 0006）。
    Complete(ctx context.Context, c CompleteRequest) error

    // Fail 记失败：递增 claim_count，按 f.Backoff 递增退避置为再次可
    // 认领；claim_count 达 f.RetryLimit 时置 failed 终态——递增计数、
    // 退避调度与终态判定在单次 Fail 内原子完成（ADR 0006）；仅当属主
    // 凭证匹配时生效（CAS），迟到的 Fail 不递增、不污染新属主状态。
    Fail(ctx context.Context, f FailRequest) error
}
```

请求/响应类型：

```go
type FetchQuery struct {
    Type  string // 单类型单次取数：编排按类型轮询限额，防单类型洪泛（ADR 0004）
    After string // 已读水位（Position），首次为空串；core 内存维护，只进不退；
            // 非空且无法解析为 Position 时 Fetch 返回错误
    Limit int
}

type ClaimQuery struct {
    Type         string
    ConsumerType string        // 认领键（design.md §5.1）
    Lease        time.Duration // 本次认领的租约有效期
    Limit        int
    RetryLimit   int           // 重试上限：达此值的接管认领直接置 failed
                               // （与 FailRequest.RetryLimit 同源，订阅级配置下推）
    Now          time.Time     // 租约过期与退避判据的时钟，由 core 下推
}

// Lease 认领结果：一次租约的属主凭证。
type Lease struct {
    Envelope     RawEnvelope
    ConsumerType string
    LeaseToken   string    // 不透明凭证；Heartbeat/Complete/Fail 必须携带（CAS 依据）
    LeaseUntil   time.Time
    Attempts     int       // 当前 claim_count（含本次认领）
}

type HeartbeatRequest struct {
    EventID      string
    ConsumerType string
    LeaseToken   string
    Extend       time.Duration // 续租时长
    Now          time.Time
}

type CompleteRequest struct {
    EventID      string
    ConsumerType string
    LeaseToken   string
    Now          time.Time
}

type FailRequest struct {
    EventID      string
    ConsumerType string
    LeaseToken   string
    RetryLimit   int     // claim_count 达此值置 failed（订阅级配置下推）
    Backoff      Backoff // 递增退避（订阅级配置下推）
    Now          time.Time
}

// Backoff 递增退避：依据已尝试次数计算下次可认领前延时（ADR 0006，
// 防热循环）。attempt 从 1 开始（首次失败 = 1）。
type Backoff interface {
    Next(attempt int) time.Duration
}

// 内建实现（可自定义 Backoff 注入）：
func LinearBackoff(base, max time.Duration) Backoff      // base * attempt，封顶 max
func ExponentialBackoff(base, max time.Duration) Backoff // base * 2^(attempt-1)，封顶 max
```

语义备注：

- Now 显式入参：判据时钟一律由 core 下推——契约测试可控时钟，后端
  判据不依赖宿主时钟。
- Complete/Fail/Heartbeat 的 CAS 不命中（租约过期、已被接管）**返回 nil
  而非错误**——「不生效」是接管场景的正常结果，是否记日志由 core 决定。
- Driver 不返回哨兵错误；错误一律视为该次操作失败，core 记日志重试或跳过。

### 2.5 Runner

```go
// Handler 消费处理函数；两种消费方式同一签名（design.md §6）。
// 返回非 nil error 即本次处理失败（广播：记日志跳过；认领：计一次失败重试）。
type Handler[T any] func(ctx context.Context, e Envelope[T]) error

// NewRunner 构造消费编排器。同一 Runner 可混合承载广播与认领订阅；
// 消费方式与订阅配置在 Event[T].SubscribeBroadcast / SubscribeClaim
// 处声明。
func NewRunner(d Driver, opts ...RunnerOption) *Runner

func WithIdentity(identity string) RunnerOption   // 认领订阅的默认消费者身份
                                                  // （design.md §5.1，订阅级可覆盖）
func WithRunnerCodec(c Codec) RunnerOption        // 缺省 JSON
func WithPollInterval(d time.Duration) RunnerOption
func WithLocalWake(w *LocalWake) RunnerOption     // 唤醒信号路由入口（ADR 0003）
func WithLogger(l *slog.Logger) RunnerOption      // 缺省 slog.Default()
```

```go
// SubscribeOption 广播与认领均可用的选项
func WithFetchLimit(n int) SubscribeOption   // 每轮按类型限额取数（ADR 0004）
func WithConcurrency(n int) SubscribeOption  // handler 并发度

// 仅认领订阅可用（广播订阅传入返回 ErrNotClaimSubscription）
func WithConsumerType(identity string) SubscribeOption  // 覆盖 Runner 默认身份
func WithLease(d time.Duration) SubscribeOption         // 租约时长（缺省 30s）
func WithHeartbeat(d time.Duration) SubscribeOption     // 心跳间隔（缺省 lease/3；
                                                        // 配置 ≥ lease 时钳制为 lease/3）
func WithMaxProcessing(d time.Duration) SubscribeOption // 最大处理时长，超过停止续租
                                                        // （缺省 10min，ADR 0006）
func WithRetryLimit(n int) SubscribeOption              // 重试上限（缺省 5）
func WithBackoff(b Backoff) SubscribeOption             // 缺省 ExponentialBackoff(1s, 1h)
```

```go
// Start 启动全部订阅的 poller。广播基线在 Start 内同步确立：对每个
// 广播订阅调用 Driver.HeadPosition，水位锚定到该类型已提交的最大位置
// ——Start 返回即基线确立，此后发布的事件必然可投递；任一锚点查询
// 失败则整体不启动并返回错误，修复后可重试 Start。重复 Start 为无操作。
func (r *Runner) Start(ctx context.Context) error

// Stop 优雅停机：停止认领/拉取，等待在途 handler 跑完（design.md §9.1）。
// 在途 handler 的 ctx 不因停机取消（停机不把在途事件记为失败）；
// Stop 的 ctx 超时是调用方兜底。
func (r *Runner) Stop(ctx context.Context) error

// Run 便捷封装：Start 后阻塞至 ctx 取消，再 Stop。
func (r *Runner) Run(ctx context.Context) error
```

编排行为（core 内部，不进签名面）：

- 广播基线在 Start 内经 `Driver.HeadPosition` 同步锚定（返回即基线确立，
  ADR 0006）；此后按订阅类型轮询 Fetch，watermark 按（订阅 × 类型）在
  内存维护，只进不退（ADR 0004）；handler 失败仅记日志跳过。
- 认领消费按订阅类型轮询 Claim，每轮每类型限额 `WithFetchLimit`，
  防单类型洪泛饿死其他类型（ADR 0004）。
- 每个在途租约由独立 goroutine 心跳续租，心跳持续至
  `min(处理结束, 最大处理时长)`；超过最大处理时长停止续租，租约到期由
  其他实例接管（ADR 0006）。心跳瞬时错误（DB 抖动）记日志并在下个间隔
  重试，不停止续租——续租的停止只由最大处理时长决定。
- 就近唤醒：Publish 经 LocalWake 发信号，Runner 按订阅注册表定向路由本
  实例对应 poller 立即拉取/认领；信号不承载投递保证，跨实例与丢失由
  poll 兜底（ADR 0003）。

### 2.6 就近唤醒总线

```go
// LocalWake 同进程唤醒总线：发布侧发信号、Runner 按订阅注册表接收。
// 仅信号，不是投递（ADR 0003）；同一 LocalWake 可同时注入 Publisher
// 与多个 Runner。
func NewLocalWake() *LocalWake
```

### 2.7 哨兵错误

```go
var (
    ErrRunnerStarted        // Runner.Start 后再订阅
    ErrNotClaimSubscription // 广播订阅传入认领专属选项
    ErrIdentityMissing      // 认领订阅未设置身份（订阅级与 Runner 默认皆缺）
)
```

## 3. Postgres 后端签名（postgrec / pgxc）

两个 Postgres 后端共享同一套能力与选项语义，仅连接载体不同：`postgrec`
基于标准库 `database/sql`（`*sql.DB`），`pgxc` 基于 pgx v5 连接池
（`*pgxpool.Pool`）；选项与 `Migrate` 在各自 module 内同名定义。

```go
package postgrec // github.com/jninng/go_event/postgrec（标准库 database/sql）

// NewDriver 基于 *sql.DB 构造 event.Driver。
func NewDriver(db *sql.DB, opts ...Option) event.Driver

// Migrate 幂等 DDL：创建 event / event_claim 两表与唯一约束
// （event(type,id)、event_claim(event_id,consumer_type)）。
func Migrate(ctx context.Context, db *sql.DB, opts ...Option) error
```

```go
package pgxc // github.com/jninng/go_event/pgxc（pgx v5）

// NewDriver 基于 pgx v5 连接池构造 event.Driver。
func NewDriver(pool *pgxpool.Pool, opts ...Option) event.Driver

// Migrate 幂等 DDL：与 postgrec.Migrate 相同的两表与唯一约束。
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error
```

两 module 各自提供的同名选项：

```go
// WithSchema 表所在 schema（缺省 public）。
func WithSchema(s string) Option

// WithRetention 保留窗口（缺省 7 天）：惰性清理资格为「超窗且无活跃
// 租约，无论终态与否」（design.md §5.2、ADR 0006）。认领消费须将窗口
// 配置为覆盖最坏重试路径：约 重试上限 × (最大退避 + 最大处理时长)，
// 另留心跳间隔与时钟偏差余量。
func WithRetention(d time.Duration) Option
```

两者均须通过 eventtest 全套契约测试并各自给出能力边界表（ADR 0005）；
pgxc 可原生实现 P1 的 LISTEN/NOTIFY 推送唤醒，postgrec 退化回轮询。

## 4. eventtest 签名

```go
package eventtest // github.com/jninng/go_event/eventtest

// RunSuite 运行全套契约测试（design.md §9.1 列举的全部语义）。
// 新后端必须全过；语义即测试。
func RunSuite(t *testing.T, d event.Driver)

// RunRetentionSuite 运行保留窗口惰性清理契约（design.md §5.2）。
// newDriver 须为每个用例返回配置了保留窗口（≤ 2s，推荐 2s）的
// 全新 Driver 实例（清理簿记按 Driver 实例记忆，共享实例会被节流跳过）。
func RunRetentionSuite(t *testing.T, newDriver func(*testing.T) event.Driver)
```

```go
package inmem // github.com/jninng/go_event/eventtest/inmem

// New 构造进程内 Driver：供契约测试与使用方本地测试/示例使用。
// 条件过滤在内存模拟，能力边界表注明「模拟」档（ADR 0005）。
func New(opts ...Option) event.Driver
```

## 5. 使用示例

### 5.1 广播消费：通知类（发完即忘）

```go
package main

import (
    "context"
    "log/slog"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/jninng/go_event"
    "github.com/jninng/go_event/pgxc"
)

type DocUploaded struct{ DocID string }

// 事件声明：包级变量，Name 是稳定标识
var docUploaded = event.Declare[DocUploaded]("doc.uploaded")

func main() {
    ctx := context.Background()

    pool, _ := pgxpool.New(ctx, dsn)
    _ = pgxc.Migrate(ctx, pool)
    drv := pgxc.NewDriver(pool, pgxc.WithRetention(7*24*time.Hour))
    // 标准库环境改用 postgrec：
    //   db, _ := sql.Open("pgx", dsn)
    //   drv := postgrec.NewDriver(db, postgrec.WithRetention(7*24*time.Hour))

    wake := event.NewLocalWake() // 同进程发布 → 本实例 poller 立即拉取

    pub := event.NewPublisher(drv,
        event.WithIDGenerator(func() string { return uuid.NewString() }),
        event.WithLocalWake(wake),
    )

    run := event.NewRunner(drv,
        event.WithPollInterval(30*time.Second),
        event.WithLocalWake(wake),
    )
    _ = docUploaded.SubscribeBroadcast(run, func(ctx context.Context, e event.Envelope[DocUploaded]) error {
        slog.Info("doc uploaded", "doc", e.Payload.DocID,
            "trace_id", e.Metadata["trace_id"])
        return nil // 失败仅记日志跳过：不重试、不保序、不承诺投递
    },
        event.WithConcurrency(8),
        event.WithFetchLimit(100),
    )

    _ = run.Start(ctx)
    defer run.Stop(ctx)

    // 主流程：发布即返回，上游无感（事件侧糖与 pub.Publish 等价）
    _ = docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"})
}
```

要点：只处理订阅启动后发布的事件，停机期间事件不补投；多实例各自独立
全收，互不影响。

### 5.2 认领消费：任务类（完成追踪 + 失败重试）

```go
type VectorizeRequest struct{ DocID string }

var vectorize = event.Declare[VectorizeRequest]("doc.vectorize")

run := event.NewRunner(drv,
    event.WithIdentity("vectorizer"), // 认领订阅的默认身份
    event.WithPollInterval(5*time.Second),
    event.WithLocalWake(wake),
)
_ = vectorize.SubscribeClaim(run, func(ctx context.Context, e event.Envelope[VectorizeRequest]) error {
    // handler 必须幂等：以 e.ID（事件 ID）为幂等键，重复投递不产生二次副作用
    return vectorizeDoc(ctx, e.Payload.DocID, e.ID)
},
    event.WithConsumerType("doc-vectorizer"), // 身份可按订阅覆盖；不设则用默认身份
    event.WithConcurrency(4),
    event.WithLease(30*time.Second),          // 租约 30s，心跳每 10s 续租
    event.WithMaxProcessing(10*time.Minute),  // 超过即停止续租，租约到期被接管
    event.WithRetryLimit(5),                  // 达上限置 failed
    event.WithBackoff(event.ExponentialBackoff(time.Second, time.Hour)),
    event.WithFetchLimit(10),
)

_ = run.Start(ctx)
```

要点：认领即抢锁，同身份同一时刻至多一个活跃租约；挂死 handler 不被
无限续保（超过最大处理时长停止心跳）；接管计入重试计数；at-least-once，
幂等是消费方责任。

### 5.3 无参事件（2×2 组合的另一角）

```go
var cacheFlush = event.Declare[event.NoPayload]("cache.flush")

// 发布：事件侧糖统一形态；亦可用 pub.PublishNoPayload(ctx, cacheFlush)
_ = cacheFlush.Publish(ctx, pub, event.NoPayload{})

// 广播订阅（与有参广播同一签名）
_ = cacheFlush.SubscribeBroadcast(run, func(ctx context.Context, e event.Envelope[event.NoPayload]) error {
    return flushCache(ctx)
})
```

### 5.4 一个进程承载多个身份

```go
run := event.NewRunner(drv, event.WithIdentity("default"),
    event.WithPollInterval(5*time.Second))

_ = vectorize.SubscribeClaim(run, vectorizeHandler,
    event.WithConsumerType("vectorizer")) // 独立认领、独立心跳

_ = thumbnail.SubscribeClaim(run, thumbnailHandler,
    event.WithConsumerType("thumbnailer")) // 各身份消费彼此独立
```

要点：终态是（事件 × 身份）粒度；新增身份从头认领全部窗口内历史
（含其他身份已 done/failed 的事件）。同一 Runner 也可混合承载广播与
认领订阅。

### 5.5 元数据透传（trace\_id 关联调用链）

```go
import "go.opentelemetry.io/otel/trace"

span := trace.SpanFromContext(ctx)
_ = docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"},
    event.WithMetadata(map[string]string{
        "trace_id": span.SpanContext().TraceID().String(),
    }))
```

### 5.6 面向测试的最小装配（inmem）

```go
import (
    "github.com/jninng/go_event/eventtest"
    "github.com/jninng/go_event/eventtest/inmem"
)

func TestVectorizeFlow(t *testing.T) {
    drv := inmem.New()
    // …用 drv 装配 Publisher/Runner，跑通生产与消费断言…
    // 契约测试侧：
    // eventtest.RunSuite(t, drv) —— 新后端验证用
}
```

## 6. 签名与设计语义对照

| 设计语义（出处）                           | 落点                                                                           |
| ---------------------------------- | ---------------------------------------------------------------------------- |
| 强类型/无参 × 广播/认领 2×2 正交（§3）          | `Declare[T]`、`NoPayload`、`SubscribeBroadcast`/`SubscribeClaim` 同一 Handler 签名 |
| Driver 最小缝合点、条件下推原子性（§4、ADR 0004）  | `FetchQuery.Type/After`、`ClaimQuery.Type/ConsumerType/Lease/Now`             |
| 广播基线 Start 同步锚定（§5.2、ADR 0006）     | `Driver.HeadPosition` + `Runner.Start`（返回即基线确立）                              |
| 广播水位内存维护、只进不退（§5.2、ADR 0004/0006）  | `RawEnvelope.Position` + `FetchQuery.After`，core 内存持有                        |
| 按类型轮询限额（§5.4）                      | 每 Type 单次 Fetch/Claim + `WithFetchLimit`                                     |
| 认领互斥、接管计入重试（§5.3、ADR 0006）         | `Claim` 语义注释 + `Lease.Attempts` + `ClaimQuery.RetryLimit`（达上限的接管置 failed）    |
| 心跳、最大处理时长、超时停续（§5.3、ADR 0006）      | `HeartbeatRequest` + `WithLease/WithHeartbeat/WithMaxProcessing`             |
| 迟到 Complete/Fail 不生效（CAS，ADR 0006） | `LeaseToken` + Complete/Fail CAS 不命中静默 nil                                   |
| 重试上限 + 递增退避（§5.3、ADR 0006）         | `FailRequest.RetryLimit/Backoff`（Driver 内原子完成）+ `WithRetryLimit/WithBackoff` |
| 幂等 Append 首写为准（§4）                 | `Append` 语义注释 + 契约测试                                                         |
| 元数据透传（§4）                          | `RawEnvelope.Metadata` + `WithMetadata`                                      |
| 就近唤醒仅信号（ADR 0003）                  | `LocalWake` + `WithLocalWake`（Publisher/Runner 双侧注入）                         |
| 消费者身份构造默认 + 订阅覆盖（§5.1）             | `WithIdentity` + `WithConsumerType`，皆缺返回 `ErrIdentityMissing`                |
| 订阅启动前注册完毕（§5）                      | 订阅入口在 Start 后返回 `ErrRunnerStarted`                                           |
| 构造器显式注入、无包级状态（§4）                  | 全部 `NewX(依赖, opts...)`；`Event[T]` 为无状态声明值                                    |
| 保留窗口惰性清理（§5.2、ADR 0006）            | `pgxc.WithRetention` / `postgrec.WithRetention`                              |
