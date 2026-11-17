# go\_event API 签名

- 状态：**已定稿（实施基线）**，2026-11-11
- 能力面与语义的单一事实源是 [design.md](design.md) 与 [adr/](adr/)；本文
  是签名定稿载体，与两者冲突时以语义文档为准并回改本文。

## 1. 包布局与类型形态

```
github.com/jninng/go_event                 # core：契约与编排（package event）
github.com/jninng/go_event/postgrec        # Postgres 后端，标准库 database/sql（package postgrec）
github.com/jninng/go_event/pgxc            # Postgres 后端，pgx v5（package pgxc）
github.com/jninng/go_event/postgresschema  # Postgres 方言 DDL 单一事实源（无驱动依赖）
github.com/jninng/go_event/eventtest       # 契约测试套件（package eventtest）
github.com/jninng/go_event/eventtest/inmem     # 进程内 Driver
github.com/jninng/go_event/eventtest/simmigrate # 迁移契约测试的进程内模拟 backend
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

// Publish 发布事件（对称糖，等价 event.Publish(ctx, p, ev, payload, opts...)；
// Go 方法不支持类型参数，Publisher 上无对应方法）。
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
    // ID 是事件主键、全局唯一（发布侧生成或 WithEventID 显式指定），
    // 幂等去重与追踪主键。跨事件类型复用同一 ID 视为同一事件，业务键
    // 须按类型加前缀后才能当 ID 用。
    ID         string
    Type       string            // 即 Event.Name
    Payload    T
    Metadata   map[string]string // 透传元数据，如 trace_id（design.md §4）
    OccurredAt time.Time
    // Attempts 是本次投递的尝试序号（含本次）：新认领为「已失败次数 + 1」，
    // 接管认领在先前基础上递增。仅认领消费有意义，广播消费恒为 0。
    Attempts    int
    // LastAttempt 表示本次是否为最后一次尝试：Attempts >= 订阅 RetryLimit
    // 时为 true（按 >= 判定，防御多实例间的订阅配置差异）。接管侧在认领时
    // 直接置 failed 的路径不投递「最后一次」，handler 不得假设它必然出现。
    // 广播消费恒为 false。
    LastAttempt bool
}

// RawEnvelope 面向 Driver 的事件信封（payload 已编码）。除运行时注入字段
// （Attempts/LastAttempt，见下）外，后端必须原样落库透传（design.md §4）。
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
    // Lease.Attempts 是 Claim 侧尝试序号的唯一来源；RawEnvelope.Attempts /
    // LastAttempt 仅由 core 在派发 handler 前写入，Append 输入侧忽略且不落库，
    // Fetch/Claim 返回时必须为零值。Driver 不得依赖它们，也不得把它们作为
    // 存取判据。
    Attempts    int
    LastAttempt bool
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
// WithEventID 优先于 IDGenerator；空串返回 ErrEmptyEventID。
// 事件侧糖 Event[T].Publish 委托到此。因 Go 方法不支持类型参数，
// Publish 为包级泛型函数而非 Publisher 方法。
func Publish[T any](ctx context.Context, p *Publisher, ev Event[T],
    payload T, opts ...PublishOption) error

// PublishNoPayload 无参事件糖，等价 Publish(ctx, ev, NoPayload{})。
func (p *Publisher) PublishNoPayload(ctx context.Context, ev Event[NoPayload],
    opts ...PublishOption) error

func WithMetadata(meta map[string]string) PublishOption // 合并进缺省元数据

// WithEventID 显式指定事件 ID，优先于 IDGenerator；空串非法，Publish
// 返回 ErrEmptyEventID（不静默回退到生成器）。
func WithEventID(id string) PublishOption
```

事件 ID 是事件主键、**全局唯一**；`Append` 以 ID 为唯一冲突目标，冲突时
静默首写为准（不校验 payload；重复发布不同 payload 保留旧值，Metadata 与
OccurredAt 同样保留首写）。**跨事件类型复用同一 ID 不报错**，而是静默视为
同一事件、丢弃第二个 payload——业务键必须按类型加前缀（或与类型绑定）后
才能当 ID 用。若确需跨类型复用同一业务键，必须改主键模型，不能只靠文档
约定；进程内串行锁不能跨实例去重，不能替代该语义。

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
    // 本次 Fail 使行进入 failed 终态时，后端把清洗后的 f.Error 摘要
    // （SanitizeErrorSummary）与时刻写入 last_error / failed_at；重试中
    // （未到终态）与迟到的 Fail 均不写摘要与时间。认领侧的接管终态
    // （达 RetryLimit 直接置 failed）只写 failed_at，不写摘要（无本次
    // 处理错误）。
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
    Attempts     int       // 本次认领的尝试序号 = 认领后 claim_count + 1
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
    // Error 是本次失败的错误文本，按不可信输入处理：后端以
    // SanitizeErrorSummary 清洗（截断至 MaxErrorSummaryBytes、去控制
    // 字符、不落堆栈）后仅在终态写入 last_error；空串视为未提供摘要
    // （last_error 置 NULL）。core 的 Runner 下推 handler 的 err.Error()。
    Error        string
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

// 错误摘要清洗（ADR 0009）：各后端写 last_error 前的统一口径。
const MaxErrorSummaryBytes = 4 << 10 // 摘要字节上限（4 KiB）

// SanitizeErrorSummary 清洗不可信错误文本：移除全部 Unicode 控制字符
// （Unicode Cc 类：C0/C1/DEL，含换行与制表符——非控制字符如 U+2028/
// U+2029 分隔符原样保留），非法字节以 UTF-8 替换符（U+FFFD）保留，按
// 字节上限截断且不撕裂 UTF-8 码点；空串输入返回空串。
func SanitizeErrorSummary(text string) string
```

语义备注：

- Now 显式入参：判据时钟一律由 core 下推——契约测试可控时钟，后端
  判据不依赖宿主时钟。
- Complete/Fail/Heartbeat 的 CAS 不命中（租约过期、已被接管）**返回 nil
  而非错误**——「不生效」是接管场景的正常结果，是否记日志由 core 决定。
- Fail 错误摘要仅终态写入：`last_error` 为最近一次终态失败的摘要（清洗
  后文本；空串视为未提供），`failed_at` 非空 ⟺ 行当前处于 `failed` 终态；
  接管终态只写 `failed_at`。口径见 design.md §5.3 与 ADR 0009。
- 除迁移相关错误（`ErrMigrationLockTimeout`、`ErrMigrationPermissionDenied`、
  `ErrMigrationUnsupported`、`ErrMigrateModeUnsupported`、`ErrInvalidSchema`
  等，见 §2.8）外，Driver 不返回哨兵错误；其余错误一律视为该次操作失败，
  core 记日志重试或跳过。迁移错误在操作入口返回，调用方应按错误性质修正
  配置、退避重试或改用 `Verify`/`None`。

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
func WithFetchLimit(n int) SubscribeOption   // 每轮按类型限额取数（ADR 0004）；
                                             // 认领侧实际批大小另受空闲并发
                                             // 槽位封顶（usage.md §6.6）
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
- 认领消费按订阅类型轮询 Claim，每轮 Limit 取 `min(WithFetchLimit, 当前
  空闲并发槽位)`，无空闲槽位时不认领；`WithFetchLimit` 对认领是批次上限，
  不要求 ≤ `WithConcurrency`。认领返回即派发、心跳随派发启动，不存在已
  认领但排队等槽位的窗口（以 Driver 遵守 Claim 至多返回 Limit 条契约为
  前提）。槽位释放后 core 立即补认领一轮，积压消化不依赖轮询间隔。
  `WithFetchLimit` 在广播订阅仍是每轮取数上限。
- 每个在途租约由独立 goroutine 心跳续租，心跳持续至
  `min(处理结束, 最大处理时长)`；超过最大处理时长停止续租，租约到期由
  其他实例接管（ADR 0006）。心跳瞬时错误（DB 抖动）记日志并在下个间隔
  重试，不停止续租——续租的停止只由最大处理时长决定。
- 就近唤醒：Publish 经 LocalWake 发信号，Runner 按订阅注册表定向路由本
  实例对应 poller 立即拉取/认领；信号不承载投递保证，跨实例与丢失由
  poll 兜底（ADR 0003）。
- 认领派发前由 core 用 `Lease.Attempts` 填充 `RawEnvelope.Attempts`，并按
  `Lease.Attempts >= 订阅 RetryLimit` 设置 `RawEnvelope.LastAttempt`；解码后
  原样映射到 `Envelope`；广播路径不设置，保持 `0` / `false`。
  接管侧在认领时直接置 failed 的路径不投递「最后一次」，handler 不得
  假设 `LastAttempt` 必然出现。
- 接管例外：进程崩溃、停机（停止派发，未开始的租约到期后由其他实例接管）
  与 handler 超过 `MaxProcessing`（停止续租）都允许接管，属 at-least-once
  的正常表现，不是异常。

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
    ErrEmptyEventID         // WithEventID 传入空串（不回退到 IDGenerator）

    // ---- 迁移（ADR 0007） ----
    ErrMigrateModeUnsupported    // WithMigrateMode 显式传给 Migrate（该选项只对 NewDriver 有意义）
    ErrMigrationUnsupported      // 锁能力为无（LockNone）时 Create 不支持自动迁移（只能 Verify/None）
    ErrMigrationLockTimeout      // 等待迁移锁超时，属瞬时错误（退避后重试）
    ErrMigrationPermissionDenied // DDL 权限不足，属永久配置错误
    ErrInvalidSchema             // schema 名非法（只允许 ^[A-Za-z_][A-Za-z0-9_]*$）
)

// IsPermanentMigrationError 报告迁移失败是否属永久配置错误：*VerifyError、
// ErrMigrationUnsupported、ErrMigrateModeUnsupported、
// ErrMigrationPermissionDenied、ErrInvalidSchema 为 true；瞬时错误
// （数据库不可用、锁等待超时）为 false。永久错误应 fail-fast，不进入重试窗口。
func IsPermanentMigrationError(err error) bool
```

### 2.8 迁移契约

迁移模式定义在 core；`WithMigrateMode` 选项函数由各后端提供（`Option` 是
后端私有类型，避免枚举在多个后端重复定义）。只有三种模式；没有 `Upgrade`
（升级随部署隐式发生，见 ADR 0007）。

```go
// MigrateMode 运行面（Driver 构造期）迁移模式；只对 NewDriver 有意义。
type MigrateMode int

const (
    MigrateCreate MigrateMode = iota // 缺省：首次操作时持迁移锁幂等应用版本计划全量语句
    MigrateVerify                    // 只读校验，不执行 DDL
    MigrateNone                      // 不建表也不校验
)

func (m MigrateMode) String() string // "create" / "verify" / "none"
```

对象与校验报告：

```go
type ObjectKind string

const (
    KindSchema     ObjectKind = "schema"
    KindTable      ObjectKind = "table"
    KindColumn     ObjectKind = "column"
    KindIndex      ObjectKind = "index"
    KindConstraint ObjectKind = "constraint"
)

type VerifyProblem string

const (
    ProblemMissing  VerifyProblem = "missing"  // 对象缺失（计划可补齐；后续版本对象缺失表示待升级）
    ProblemOutdated VerifyProblem = "outdated" // 定义与计划期望不一致（不自动修复）
    ProblemTooNew   VerifyProblem = "too_new"  // 预留：当前模型（对象集合即事实）不产生
    ProblemFailed   VerifyProblem = "failed"   // 预留：迁移台账已否决，当前模型不产生
)

type VerifyIssue struct {
    Object  string // 稳定标识，如 "public"、"public.event"、"public.event.id"
    Kind    ObjectKind
    Problem VerifyProblem
    Action  string // 处置说明；契约测试只断言字段，不断言文本
}

type VerifyReport struct {
    Issues []VerifyIssue // 为空表示与基线一致
}

func (r VerifyReport) OK() bool

// VerifyError 在 Verify 发现问题时返回，携带结构化报告；
// errors.As 可取回 *VerifyError 后读取 Report。
type VerifyError struct {
    Report VerifyReport
}

func (e *VerifyError) Error() string
```

能力声明（供契约测试与文档判定）：

```go
type DDLAtomicity int

const (
    DDLAtomicNone          DDLAtomicity = iota // 无 DDL 原子性：不支持自动迁移
    DDLAtomicSingle                            // 仅单语句原子
    DDLAtomicTransactional                     // 跨语句事务
)

type NativeSupport int

const (
    SupportNone     NativeSupport = iota // 无原生 IF NOT EXISTS
    SupportWarnOnly                      // 接受语法但告警/有限支持
    SupportNative                        // 原生支持
)

type ProbeSupport int

const (
    ProbeNone       ProbeSupport = iota // 不可探测
    ProbeExistence                      // 只能探测存在性
    ProbeDefinition                     // 可探测存在性并比对定义
)

type LockPrimitive int

const (
    LockNone        LockPrimitive = iota // 无锁：Create 不支持自动迁移
    LockRowLease                         // 锁行租约（TTL、续约、过期回收）
    LockSession                          // 原生会话锁（固定连接、断连释放）
    LockTransaction                      // 事务级锁（随提交/回滚释放）
)

type LockRelease int

const (
    LockReleaseTransaction LockRelease = iota // 提交/回滚自动释放（零值）
    LockReleaseSession                        // 会话断连释放
    LockReleaseLease                          // 租约到期
)

type DDLCompletion int

const (
    DDLSync  DDLCompletion = iota // DDL 返回即完成
    DDLAsync                      // DDL 可能返回异步 job
)

type ErrorClasses struct {
    Exists     bool // 对象已存在/重复定义
    Permission bool // 权限不足
    Transient  bool // 瞬时错误（可退避重试）
}

type MigrationCapabilities struct {
    Atomicity             DDLAtomicity
    IfNotExists           map[ObjectKind]NativeSupport // 按对象类型的原生 IF NOT EXISTS 覆盖
    Probe                 map[ObjectKind]ProbeSupport  // 按对象类型的探测能力
    Errors                ErrorClasses
    Lock                  LockPrimitive
    LockRelease           LockRelease
    Completion            DDLCompletion
    ImplicitCommit        bool // DDL 是否隐式提交（放进事务也无法整体回滚）
    NonTransactionalSteps bool // 计划可包含必须在事务外执行的语句（如并发建索引）
}
```

故障点：迁移执行器可注入故障的语句边界，由迁移契约测试（`simmigrate`
与真实后端的 `InjectFault`）使用。

```go
type FaultPoint string

const (
    FaultAfterLock       FaultPoint = "after_lock"
    FaultBeforeStatement FaultPoint = "before_statement"
    FaultAfterStatement  FaultPoint = "after_statement"
    FaultBeforeCommit    FaultPoint = "before_commit"
)
```

### 2.9 可观测（otel 指标 + slog 日志 + Observer）

指标走 otel、日志走 slog，默认跟随全局装配（未配置时 otel 全局 provider
即 noop，静默零成本）；选项优先、全局兜底，构造期快照固定。决策与口径
分界见 [ADR 0008](adr/0008-observability.md)。

```go
// PublisherOption 与 RunnerOption 双面选项（同 WithLocalWake 形态）：
func WithMeterProvider(mp metric.MeterProvider) // 缺省构造期快照 otel.GetMeterProvider()
func WithScopeName(scope string)                // 缺省 "go_event"；部署期决策，勿随实例变化
func WithObserver(o Observer)                   // 缺省无观察者

// Runner 已有：
func WithLogger(l *slog.Logger) // 缺省 slog.Default()

// 观测旁路：回调同步执行、须微秒级返回；库内 recover panic。
type Observer interface {
    OnPublish(PublishEvent)
    OnConsume(ConsumeEvent)
    OnWake(WakeEvent)
}

type ObserverV2 interface { // 可选扩展，分发点类型断言
    Observer
    OnLeaseAbandoned(LeaseEvent) // 停机放弃租约（待接管）
    OnRunnerStopped(StopEvent)   // TimedOut=true 表示等待在途超过调用方 ctx
}

type PublishEvent struct{ Event, Result string; Err error }
type ConsumeEvent struct{ Mode, Event, Result string; Attempts int; Err error }
type WakeEvent struct{ Event string; Notified, Dropped int }
type LeaseEvent struct{ Event string }
type StopEvent struct{ TimedOut bool }
```

`Result` 枚举：`ok` / `invalid_id` / `encode_error` / `append_error`（发布）；
`ok` / `handler_error` / `handler_panic`（消费）；`error` / `canceled`（轮询）。
`Mode`：`broadcast` / `claim`。

**指标清单**（`<scope>.<动作>`；计数单位 `1`，耗时直方图单位 `s`；Prometheus
导出侧自动转 `.`→`_` 并按单位追加 `_total`/`_seconds`）：

| 指标 | 类型 | 属性 |
| --- | --- | --- |
| `go_event.publish` | Counter | `event`, `result` |
| `go_event.publish.duration` | Histogram | `result`（仅 Append 已尝试时记录） |
| `go_event.fetch` / `.duration` | Counter + Histogram | `result`=ok/error/canceled |
| `go_event.claim` / `.duration` | Counter + Histogram | `result`=ok/error/canceled |
| `go_event.claim.granted` | Counter | `event`, `identity` |
| `go_event.lease.abandoned` | Counter | `event` |
| `go_event.consume` | Counter | `mode`, `event`, `result` |
| `go_event.handle.duration` | Histogram | `mode`, `event`, `result` |
| `go_event.heartbeat` / `complete` / `fail` | Counter | `result`=ok/error |
| `go_event.wake` | Counter | `result`=sent/dropped |

属性值仅枚举与标识符（`identity` 为消费者身份，多实例区分维度）；业务
payload（事件 ID、错误原文）不入属性。日志仅低频生命周期事件
（`runner started/stopped`、`stopping, lease released for takeover` 等），
错误经 `error.type` + `error` 字符串化上报。

后端（pgxc / postgrec）只埋迁移生命周期，发射点单一（包级 `Migrate` /
`Verify`，driver ensure 同路径无重复计量）：

```go
func WithLogger(l *slog.Logger) Option          // 缺省 slog.Default()
func WithMeterProvider(mp metric.MeterProvider) Option // 缺省 otel.GetMeterProvider()
func WithScopeName(scope string) Option         // 缺省模块名

// 指标：<scope>.migrate（Counter）+ <scope>.migrate.duration（Histogram），
// 属性 result=ok/canceled/lock_timeout/permission_denied/verify_conflict/
// permanent/transient 与 schema；Migrate 成功打 Info 日志「migration applied」
//（schema、mode、duration_seconds）。
```

## 3. Postgres 后端签名（postgrec / pgxc）

两个 Postgres 后端共享同一套能力与选项语义，仅连接载体不同：`postgrec`
基于标准库 `database/sql`（`*sql.DB`），`pgxc` 基于 pgx v5 连接池
（`*pgxpool.Pool`）；选项与迁移函数（`Migrate`/`Verify`/`Statements`/
`SQL`/`Capabilities`）在各自 module 内同名定义。

```go
package postgrec // github.com/jninng/go_event/postgrec（标准库 database/sql）

// NewDriver 基于 *sql.DB 构造 event.Driver。缺省迁移模式 Create：首次操作
// 时持迁移锁幂等应用版本计划全量语句。WithMigrateMode 只对本构造函数生效。
func NewDriver(db *sql.DB, opts ...Option) event.Driver

// Migrate 幂等应用版本计划（Create 语义）：单事务内取迁移锁、做接管校验
// （对象集合与计划期望比对，仅缺对象放行）、执行计划内全部语句（基线与
// 各版本，全部幂等），任何一步失败整体回滚；中断后重跑即收敛。可重复执行。
// 需要 DDL 权限；升级随部署隐式发生。显式传入 WithMigrateMode 返回
// ErrMigrateModeUnsupported。
func Migrate(ctx context.Context, db *sql.DB, opts ...Option) error

// Verify 只读校验：不执行 DDL，比对 schema/表/列/主键/唯一/索引/外键是否
// 与计划期望（全量版本）一致。缺失表示待升级；定义冲突返回报告与
// *event.VerifyError；查询本身失败时返回错误。只依赖表已由部署流程创建，
// 运行账号可只授 DML（另需系统目录读）。
func Verify(ctx context.Context, db *sql.DB, opts ...Option) (event.VerifyReport, error)

// Statements 返回计划内全部语句（不含迁移锁），按执行顺序排列；与执行器
// 共用同一事实源（postgresschema.Plan）。
func Statements(opts ...Option) ([]string, error)

// SQL 返回手工脚本：以 BEGIN 开始，随后设 lock_timeout 并取迁移锁（锁键
// 含 schema，锁语句位于全部 DDL 之前），随后依次输出计划内全部语句
// （基线 + 各版本，全部幂等），末尾 COMMIT；整段须在同一事务执行、整段
// 可重放，人工执行不得拆分跳过。
func SQL(opts ...Option) (string, error)

// Capabilities 返回本后端的迁移能力声明（供契约测试与能力矩阵读取）。
func Capabilities() event.MigrationCapabilities
```

```go
package pgxc // github.com/jninng/go_event/pgxc（pgx v5）

// NewDriver 基于 pgx v5 连接池构造 event.Driver。迁移模式语义与 postgrec
// 相同（缺省 Create）；WithMigrateMode 只对本构造函数生效。
func NewDriver(pool *pgxpool.Pool, opts ...Option) event.Driver

// Migrate 幂等应用版本计划（Create 语义），与 postgrec.Migrate 相同的
// 接管校验、整版单事务与失败回滚语义。
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error

// Verify 只读校验，语义与 postgrec.Verify 相同。
func Verify(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (event.VerifyReport, error)

// Statements / SQL 语义与 postgrec 相同。
func Statements(opts ...Option) ([]string, error)
func SQL(opts ...Option) (string, error)

// Capabilities 返回本后端的迁移能力声明。
func Capabilities() event.MigrationCapabilities
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

// WithMigrateMode 设置运行面迁移模式（缺省 Create）：Create 首次操作时
// 幂等应用版本计划全量语句；Verify 只读校验、不执行 DDL；None 不建表也
// 不校验。该选项只对 NewDriver 有意义，显式传给 Migrate 返回
// ErrMigrateModeUnsupported。
func WithMigrateMode(m event.MigrateMode) Option
```

迁移相关语义：

- 两个后端共享 `postgresschema` 方言包中的版本计划、期望对象与共享执行器
  （单一事实源），各自只做连接适配与错误分类。
- `Migrate` 即 `Create` 语义：以调用方连接执行 DDL，需要 DDL 权限；运行账号
  只授 DML 时不能自动迁移，应在部署期建表、运行期用 `Verify` 或 `None`。
  锁能力为无（`LockNone`）时 `Create` 返回 `ErrMigrationUnsupported`；DDL
  原子性为无（`DDLAtomicNone`）时同样返回 `ErrMigrationUnsupported`。
- 迁移协议（单一执行路径、幂等语句、无台账、接管校验、手工脚本）见
  ADR 0007。
- `Verify` 的问题类别为 `missing`（缺失：待升级，处置为执行迁移）与
  `outdated`（定义冲突：不自动修复）；`too_new` / `failed` 为预留枚举，
  当前模型不产生。
- `Statements` 不含锁；`SQL` 以 `BEGIN` 开始，随后设 `lock_timeout` 并取
  迁移锁（锁语句位于全部 DDL 之前），随后依次输出计划内全部语句，末尾
  `COMMIT`。导出覆盖版本计划全量（基线 + 各版本），与执行器共用同一份
  语句定义。
- 两个 Postgres 后端的能力声明均为：跨语句事务、schema/table/column/index
  原生 `IF NOT EXISTS`（constraint 无原生语法，用探测 + DO 块）、schema 仅
  存在性探测而表/列/索引/约束为存在性 + 定义探测、错误分类全支持、事务级
  advisory 锁、提交/回滚释放、同步完成、可包含事务外语句、不隐式提交
  （矩阵见 design.md §8）。

两者均须通过 eventtest 全套契约测试并各自给出能力边界表（ADR 0005）；
pgxc 可原生实现 P1 的 LISTEN/NOTIFY 推送唤醒，postgrec 退化回轮询。

## 4. eventtest 签名

```go
package eventtest // github.com/jninng/go_event/eventtest

// RunSuite 运行全套契约测试（design.md §9.1 列举的全部语义）。
// 新后端必须全过；语义即测试。d 须实现 ClaimStateReader
// （错误摘要落库事实是 Driver 契约的一部分）。
func RunSuite(t *testing.T, d event.Driver)

// ClaimState 认领行的落库事实快照（事件 × 消费者身份）：空串/零值
// 表示对应列为 NULL（未提供/未写入）。
type ClaimState struct {
    Status    string    // 行状态：active / retrying / done / failed
    LastError string    // 错误摘要（清洗后文本）
    FailedAt  time.Time // 行进入 failed 终态的时刻
}

// ClaimStateReader 是认领行落库事实的观察接口：RunSuite 据此断言 Fail
// 错误摘要与 failed_at 的写入时机与口径（design.md §5.3、ADR 0009）。
// inmem 原生实现；Postgres 后端在测试装配处以 SQL 适配 event_claim 行。
type ClaimStateReader interface {
    // ReadClaimState 返回认领行的当前落库事实；行不存在时 ok 为 false。
    // 行缺失以外的查询失败属测试装配故障，经 t 报错终止。
    ReadClaimState(t *testing.T, eventID, consumerType string) (ClaimState, bool)
}

// RunRetentionSuite 运行保留窗口惰性清理契约（design.md §5.2）。
// newDriver 须为每个用例返回配置了保留窗口（≤ 2s，推荐 2s）的
// 全新 Driver 实例（清理簿记按 Driver 实例记忆，共享实例会被节流跳过）。
func RunRetentionSuite(t *testing.T, newDriver func(*testing.T) event.Driver)

// RunMigrationSuite 运行迁移契约测试。suite 本身不读环境变量；门控
// （如 TEST_POSTGRES_DSN）在具体后端的测试内完成。
func RunMigrationSuite(t *testing.T, b MigrationBackend)

// ErrUnsupportedByBackend 表示后端不支持某项可选能力（强制路径 / 故障注入）。
var ErrUnsupportedByBackend = errors.New("eventtest: backend does not support")

// MigrationScene 是迁移契约测试的场景。
type MigrationScene int

const (
    MigrationSceneEmpty    MigrationScene = iota // 空库（无 schema、无表）
    MigrationSceneComplete                       // 完整基线（对象集合与定义全部一致）
    MigrationScenePartial                        // 部分存量：已有对象定义一致，仅缺对象
    MigrationSceneConflict                       // 已有对象定义冲突（不自动修复）
)

func (s MigrationScene) String() string // "empty" / "complete" / "partial" / "conflict"

// MigrationBackend 是迁移契约测试的后端适配接口，由各后端测试提供实现。
// 每个子测试都会获得独立环境：先 Reset 清空，再 Prepare 构造场景，
// 结束时 Cleanup 释放资源。
type MigrationBackend interface {
    NewDriver(t *testing.T, mode event.MigrateMode) event.Driver
    Migrate(ctx context.Context) error
    Verify(ctx context.Context) (event.VerifyReport, error)
    Statements() ([]string, error) // 计划内全部语句，不含迁移锁与账号授权
    SQL() (string, error)          // 手工脚本：BEGIN 后设 lock_timeout 并取迁移锁
    Capabilities() event.MigrationCapabilities
    Prepare(t *testing.T, scene MigrationScene)
    Reset(t *testing.T)
    // InjectFault 在指定语句边界注入错误（err 为 nil 表示清除）；
    // 不支持时返回 ErrUnsupportedByBackend。
    InjectFault(point event.FaultPoint, err error) error
    Cleanup(t *testing.T)
}
```

可选观察接口（真实后端通常不实现，模拟 backend 实现；suite 用类型断言探测）：

```go
// ApplyObserver 统计每个对象被成功应用的次数（按限定标识）。
type ApplyObserver interface {
    ApplyCount(object string) int
    ResetApplyCounts()
}

// FaultObserver 报告本次 Migrate 已尝试的语句数（失败或回滚后仍保留）。
type FaultObserver interface {
    AttemptedStatements() int
    ResetAttempts()
}

// LockObserver 统计迁移锁争用次数；配合 EntryHooker 才能稳定观察到争用。
type LockObserver interface {
    LockContentions() int
    ResetLockStats()
}

// EntryHooker 在 Migrate 入口安装钩子，让并发用例确定性地观察锁争用。
type EntryHooker interface {
    SetEntryHook(func())
}

// LockNoneProvider 返回一个声明锁能力为无（LockNone）的等价 backend，
// 用于验证该能力面不支持自动迁移。
type LockNoneProvider interface {
    LockNoneBackend(t *testing.T) MigrationBackend
}

// ObjectDescriber 返回基线期望对象的限定名列表。
type ObjectDescriber interface {
    ExpectedObjects() []string
}

// RelationChecker 报告限定标识对应的关系是否存在。
type RelationChecker interface {
    RelationExists(object string) bool
}

// SchemaNamer 返回 backend 当前使用的 schema 名。
type SchemaNamer interface {
    Schema() string
}

// SchemaExporter 按显式 schema 导出语句，供非法 schema 用例使用。
type SchemaExporter interface {
    StatementsForSchema(schema string) ([]string, error)
    SQLForSchema(schema string) (string, error)
}
```

```go
package simmigrate // github.com/jninng/go_event/eventtest/simmigrate

// New 构造进程内模拟 backend：用内存 catalog 模拟 Postgres 的
// schema/表/列/约束/索引，始终运行；实现 MigrationBackend，支持全部
// FaultPoint 注入。语句与期望对象直接消费 postgresschema，不复制 DDL 文本。
func New() *Backend
```

```go
package inmem // github.com/jninng/go_event/eventtest/inmem

// New 构造进程内 Driver：供契约测试与使用方本地测试/示例使用。
// 条件过滤在内存模拟，能力边界表注明「模拟」档（ADR 0005）。
func New(opts ...Option) event.Driver

// Capabilities 返回零值能力声明：无持久结构，迁移为 no-op（无 DDL
// 原子性、无锁、无探测、无错误分类）。
func Capabilities() event.MigrationCapabilities

// Migrate 是 no-op：不产生任何 DDL，恒返回 nil。
func Migrate(context.Context) error

// Verify 是 no-op：恒报告 OK 且无错误。
func Verify(context.Context) (event.VerifyReport, error)

// ReadClaimState 返回认领行的落库事实（事件 × 消费者身份），实现
// eventtest.ClaimStateReader：状态、错误摘要与终态时刻；行不存在时
// ok 为 false。
func (d *Driver) ReadClaimState(t *testing.T, eventID, consumerType string) (eventtest.ClaimState, bool)
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
    // 部署/启动期用高权限连接执行；运行账号仅 DML 时改用
    // pgxc.WithMigrateMode(event.MigrateVerify) 或 event.MigrateNone。
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

    // 主流程：发布即返回，上游无感（事件侧糖与 event.Publish 等价）
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
| 持久结构初始化按模式（§8、ADR 0007）             | `MigrateMode` + 后端 `WithMigrateMode`/`Migrate`/`Verify`；缺省 Create                     |
| 迁移能力声明用于契约测试与文档判定（§8、ADR 0007） | `MigrationCapabilities` + `FaultPoint` + `Capabilities()`                          |
| 校验报告结构化、错误可 `errors.As`（§8）          | `VerifyReport`/`VerifyIssue`/`VerifyProblem` + `*VerifyError`                        |
| 保留窗口惰性清理（§5.2、ADR 0006）            | `pgxc.WithRetention` / `postgrec.WithRetention`                              |
