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

- **必选接口面只有 `Driver` 一个**（core 是它的消费方，接口定义在 core）；
  查询面 `Inspector` 为只读可选接口，按能力探测接入（见 §2.10）。
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
//
// 注册在 Runner 生命周期内任意时刻可用（Stop 前，ADR 0015）：Start 前
// 注册的订阅由 Start 统一装配；启动后注册即时生效——基线锚定于注册
// 调用内，注册返回即基线确立，只处理注册之后发布的事件；锚定查询失败
// 返回错误（包装底层错误）、订阅不生效，修复后可重试。Stop 后注册返回
// ErrRunnerStopped。
func (ev Event[T]) SubscribeBroadcast(r *Runner, h Handler[T],
    opts ...SubscribeOption) error

// SubscribeClaim 注册认领订阅（ADR 0006）。身份取 WithConsumerType，
// 未设置时用 Runner 构造默认身份（WithIdentity）；两者皆缺返回
// ErrIdentityMissing。
//
// 注册在 Runner 生命周期内任意时刻可用（Stop 前，ADR 0015）：启动后
// 注册即时生效，起点对齐新身份口径——从当前可认领行（保留窗口内，
// 含其他身份已 done/failed 的）开始认领（design.md §5.1）。Stop 后注册
// 返回 ErrRunnerStopped。
func (ev Event[T]) SubscribeClaim(r *Runner, h Handler[T],
    opts ...SubscribeOption) error
```

订阅注册在 Runner 生命周期内任意时刻可用（Stop 前）；Stop 后调用返回
`ErrRunnerStopped`（Runner 一次性，ADR 0015）。启动后注册的阻塞与失败
语义：广播订阅在注册调用内同步执行基线锚定查询（使用 Runner 运行期
context），返回即基线确立；失败则订阅不生效、既有订阅不受影响，修复后
可重试。

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
    // DeliverAt 是发布侧声明的最早投递时刻（WithDeliverAt 设置）；零值
    // 表示立即可投递。广播消费下延迟行可能被水位越过而不再投递
    // （ADR 0013），需要定时必达用认领消费。
    DeliverAt time.Time
    // CoalesceKey 是合并投递键（WithCoalesceKey 设置，ADR 0014）：同
    // (Type, CoalesceKey) 作用域内最新一次发布取代之前的待消费前驱，
    // 被取代行不再投递。空串表示未设置。与事件 ID 正交：同 ID 幂等
    // Append 语义不变（重复 Append 不触发取代）。
    CoalesceKey string
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
    // DeliverAt 是最早投递时刻（零值 = 立即可投递）：后端原样落库（零值
    // 存 NULL），Fetch / Claim 以 q.Now 下推判定 deliver_at <= now，未到期
    // 行在单条操作内被过滤（ADR 0013）。过去时刻等价立即可投递，不报错。
    DeliverAt time.Time
    // CoalesceKey 是合并投递键（ADR 0014）：后端原样落库（空串存 NULL）。
    // 同 (Type, CoalesceKey) 作用域内，Append 原子将「无任何认领行的待
    // 消费前驱」置为 superseded，Fetch / Claim 不再返回被取代行（广播
    // 水位直接越过，无新边界）；与事件 ID 正交——重复 Append 同 ID 不
    // 触发取代（幂等语义不变）。空串表示未设置，不参与合并。
    CoalesceKey string
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

// PublishNoPayload 无参事件糖，等价 Publish(ctx, p, ev, NoPayload{})。
func (p *Publisher) PublishNoPayload(ctx context.Context, ev Event[NoPayload],
    opts ...PublishOption) error

func WithMetadata(meta map[string]string) PublishOption // 合并进缺省元数据

// WithDeliverAt 声明最早投递时刻（延迟事件，ADR 0013）：到期前行不满足
// Fetch / Claim 的下推条件，到期后照常可投递。零值（选项缺省）= 立即
// 可投递；过去时刻等价立即，不报错。仅声明「不早于」语义：投递本身仍
// 经轮询/认领路径，不保证恰在该时刻发生。广播消费下延迟行可能被内存
// 水位越过而不再投递（ADR 0013），需要定时必达用认领消费。
func WithDeliverAt(t time.Time) PublishOption

// WithCoalesceKey 声明合并投递键（ADR 0014）：同 (Type, CoalesceKey)
// 作用域内，Append 原子将「无任何认领行的待消费前驱」置为 superseded
// 并插入新行——同键短期多次发布只消费最新一次。空串（选项缺省）= 未
// 设置，不参与合并；处理中 / 等退避 / 租约过期待接管 / 终态的前驱不
// 取代，新事件照常排队（v1 保守口径）。键与事件 ID 正交：同 ID 幂等
// Append 语义不变（重复 Append 不触发取代——去抖配方须每次发布新 ID，
// 见 usage.md §4.3）。窗口语义不内建：去抖经与 WithDeliverAt 组合表达
// （合并键 + deliver_at = now + 静默期，每次新发布自带基线）。被取代行
// 照常占 position，广播水位直接越过。
func WithCoalesceKey(key string) PublishOption

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

### 2.4 Driver（必选接口，最小缝合点）

```go
// Driver 单次原子操作（design.md §4、ADR 0004）：全部编排逻辑在 core 的
// Runner 完成。所有方法语义由 eventtest 契约测试约束。
type Driver interface {
    // Append 幂等：重复 Append 同 ID 视为成功（首写为准，不覆盖既有行、
    // 不触发重投，不校验 payload 一致性）——幂等与合并投递正交，重复
    // Append 不触发取代（ADR 0014）。CoalesceKey 非空时原子执行「置同键
    // 待消费前驱 superseded + 插入新行」（单条写路径）：前驱 = (Type,
    // CoalesceKey) 作用域内无任何认领行的事件行，处理中 / 等退避 / 租约
    // 过期待接管 / 终态行均不取代；被取代行照常占 position，Fetch /
    // Claim 不再返回。返回 AppendResult 汇总本次落库事实（取代行数），
    // 供观测面计量（design.md §9.2）；错误时返回零值 AppendResult。
    Append(ctx context.Context, e RawEnvelope) (AppendResult, error)

    // Fetch 广播消费读取：Type 匹配且 Position 严格大于 q.After 的行，
    // 按 Position 升序返回至多 q.Limit 条。只读，不产生任何簿记。
    // q.After 为空串表示从头；非空但无法解析为 Position 时返回错误。
    // 延迟事件（DeliverAt 非零）在到期前不满足下推条件：以 q.Now 判定
    // deliver_at <= now，过滤在单条读取内完成（ADR 0013）。被取代行
    // （superseded，ADR 0014）不满足下推条件——广播水位直接越过，无新
    // 边界。
    Fetch(ctx context.Context, q FetchQuery) ([]RawEnvelope, error)

    // HeadPosition 广播基线锚点：返回该类型当前已提交事件的最大
    // Position，无事件返回空串。core 在 Runner.Start 时对每个广播
    // 订阅调用一次，水位锚定到该快照，Start 返回即基线确立（ADR 0006）；
    // 启动后动态注册广播订阅时同样调用（基线锚定于注册调用内，注册
    // 返回即基线确立，ADR 0015）。
    HeadPosition(ctx context.Context, typ string) (string, error)

    // Claim 认领消费：原子认领至多 q.Limit 条「无主、租约过期、退避期满，
    // 且 DeliverAt 已到期，且未被取代」的行（Postgres 实现为 FOR UPDATE
    // SKIP LOCKED）。已 done / failed 的行不可认领；接管（租约过期）认领
    // 递增 claim_count（ADR 0006）；达 q.RetryLimit 的接管认领置 failed
    // 终态、不再认领（终态判定需认领侧获知上限，故由 core 随条件下推）。
    // 批次内顺序不承诺（契约仅约束 Fetch 升序）。返回 ClaimResult 汇总
    // 本轮落库事实（发放租约与接管/终态计数，计数与落库状态一致，
    // ADR 0011）。被取代行不进拾取窗口（superseded 是事件行属性，对新
    // 消费者身份同样生效，ADR 0014）。
    Claim(ctx context.Context, q ClaimQuery) (ClaimResult, error)

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
// AppendResult 一次 Append 的落库事实汇总（design.md §9.2、ADR 0014）：
// Superseded 为本次原子取代（置 superseded）的同键待消费前驱行数。
// 重复 Append 同 ID（幂等冲突）与未设置 CoalesceKey 的 Append 恒为 0；
// 计数与落库事实严格一致，可经 Inspector.CountStates 的 Superseded 档
// 对账；错误时返回零值。
type AppendResult struct {
    Superseded int
}

type FetchQuery struct {
    Type  string // 单类型单次取数：编排按类型轮询限额，防单类型洪泛（ADR 0004）
    After string // 已读水位（Position），首次为空串；core 内存维护，只进不退；
            // 非空且无法解析为 Position 时 Fetch 返回错误
    Limit int
    // Now 是延迟事件（DeliverAt）到期判据的时钟，由 core 下推。零值时
    // 任何非零 DeliverAt 的行均不可见（判据时钟未推进）。
    Now time.Time
}

type ClaimQuery struct {
    Type         string
    ConsumerType string        // 认领键（design.md §5.1）
    Lease        time.Duration // 本次认领的租约有效期
    Limit        int
    RetryLimit   int           // 重试上限：达此值的接管认领直接置 failed
                               // （与 FailRequest.RetryLimit 同源，订阅级配置下推）
    Now          time.Time     // 租约过期、退避与延迟投递（deliver_at）判据的时钟，由 core 下推
}

// ClaimResult 一次认领轮的结果与落库事实汇总（design.md §9.2、ADR 0011）。
// 计数与落库状态严格一致，供观测面计量与 Inspector 对账。
type ClaimResult struct {
    Leases         []Lease // 本次发放的租约（至多 q.Limit 条；批次内顺序不承诺）
    Takeovers      int     // 接管发放数量（含接管后直接置 failed 的终态接管；
                            // 无主首认与 Fail 退避期满的重认领不计入）
    TerminalFailed int     // 认领动作因 claim_count 达 RetryLimit 直接置
                            // failed 的数量；一次终态接管同时计入两者
}

// Lease 认领结果：一次租约的属主凭证。租约不承载接管标记——接管与
// 终态计数随 ClaimResult 按轮汇总返回（ADR 0011）；Attempts 表达
// 「一次处理尝试未完成」的累计（显式 Fail 与接管各计一次），不能当作
// 接管计数（usage.md §6.7）。
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
  判据不依赖宿主时钟。延迟投递同口径：Fetch 以 `FetchQuery.Now`、
  Claim 以 `ClaimQuery.Now` 判定 `deliver_at <= now`（含到时刻本身），
  未到期行不进结果集/拾取窗口（ADR 0013）；保留窗口自可投递时刻起算
  （`COALESCE(deliver_at, occurred_at)`）。
- Complete/Fail/Heartbeat 的 CAS 不命中（租约过期、已被接管）**返回 nil
  而非错误**——「不生效」是接管场景的正常结果，是否记日志由 core 决定。
- Fail 错误摘要仅终态写入：`last_error` 为最近一次终态失败的摘要（清洗
  后文本；空串视为未提供），`failed_at` 非空 ⟺ 行当前处于 `failed` 终态；
  接管终态只写 `failed_at`。口径见 design.md §5.3 与 ADR 0009。
- Claim 计数口径（ADR 0011）：`Takeovers` 只计「接管过期租约」的发放
  （含终态接管）；Fail 退避期满的重认领（retrying，Fail 时已计入）与
  无主首认不计入。`TerminalFailed` 独立统计认领动作直接置 failed 的数量；
  当前认领语义下置终态的下推条件要求租约已过期，故认领侧终态必经接管
  路径，恒有 `TerminalFailed ≤ Takeovers`。计数与落库状态严格一致，可经
  `Inspector.ClaimState` / claim\_count 对账；`Attempts > 1` 混合 Fail
  重试与过期接管，不能代替接管计数。
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
// Start 启动全部已注册订阅的 poller。广播基线在 Start 内同步确立：对每个
// 广播订阅调用 Driver.HeadPosition，水位锚定到该类型已提交的最大位置
// ——Start 返回即基线确立，此后发布的事件必然可投递；任一锚点查询
// 失败则整体不启动并返回错误，修复后可重试 Start。重复 Start 为无操作。
// 启动后可继续经 SubscribeBroadcast / SubscribeClaim 动态注册订阅，
// 即时生效（ADR 0015）。
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
  ADR 0006）；启动后动态注册的广播订阅在注册调用内锚定，语义同构
  （ADR 0015）。此后按订阅类型轮询 Fetch，watermark 按（订阅 × 类型）在
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

// NotifyAll 向本实例全部已订阅类型发唤醒信号（非阻塞；跨实例唤醒的
// 桥接入口，ADR 0013）：支持原生推送的后端（pgxc Wake，§3）收到 NOTIFY
// 后经此注入信号，语义与发布侧内部信号一致——仅信号、不承载投递保证，
// 丢弃与错过的信号由周期 poll 兜底（ADR 0003 边界不变）。返回送达与
// 丢弃的订阅通道数。并发安全。
func (w *LocalWake) NotifyAll() (notified, dropped int)
```

### 2.7 哨兵错误

```go
var (
    ErrRunnerStopped        // Runner.Stop 后再订阅（Runner 一次性，ADR 0015）
    ErrNotClaimSubscription // 广播订阅传入认领专属选项
    ErrIdentityMissing      // 认领订阅未设置身份（订阅级与 Runner 默认皆缺）
    ErrEmptyEventID         // WithEventID 传入空串（不回退到 IDGenerator）

    // ---- 迁移（ADR 0007） ----
    ErrMigrateModeUnsupported    // WithMigrateMode 显式传给 Migrate（该选项对 NewDriver / NewStore 的内部 Driver 构造期生效）
    ErrMigrationUnsupported      // 锁能力为无（LockNone）时 Create 不支持自动迁移（只能 Verify/None）
    ErrMigrationLockTimeout      // 等待迁移锁超时，属瞬时错误（退避后重试）
    ErrMigrationPermissionDenied // DDL 权限不足，属永久配置错误
    ErrInvalidSchema             // schema 名非法（只允许 ^[A-Za-z_][A-Za-z0-9_]*$）

    // ---- 管理面（ADR 0010） ----
    ErrAdminLockTimeout // Admin 写操作等待命名空间互斥锁超时（锁与迁移共用），属瞬时错误
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
// MigrateMode 运行面（Driver 构造期）迁移模式；对 NewDriver / NewStore
// （内部 Driver 构造期）生效。
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

type ObserverV3 interface { // 可选扩展（管理面写操作），分发点类型断言
    ObserverV2
    OnAdminReplay(AdminReplayEvent) // 管理面重放（操作来源记录，ADR 0010）
}

type ObserverV4 interface { // 可选扩展（认领轮计数），分发点类型断言
    ObserverV3
    OnClaim(ClaimEvent) // 认领轮的发放与接管/终态计数（ADR 0011）
}

type PublishEvent struct{ Event, Result string; Err error }
type ConsumeEvent struct{ Mode, Event, Result string; Attempts int; Err error }
type WakeEvent struct{ Event string; Notified, Dropped int }
type LeaseEvent struct{ Event string }
type StopEvent struct{ TimedOut bool }

// ClaimEvent 是一次认领轮的观测事件（随 Driver.Claim 的 ClaimResult 返回，
// 与落库事实一致；仅成功轮次分发，失败轮次经 <scope>.claim 指标与日志可见）。
type ClaimEvent struct {
    Event, Identity string
    Leases, Takeovers, TerminalFailed int
}

// AdminReplayEvent 是一次管理面重放的观测事件（后端 Admin 经 WithObserver
// 注入发射；EventID × ConsumerType 为重放对象标识）。
type AdminReplayEvent struct {
    EventID      string
    ConsumerType string
    Result       string // Admin 重放结果枚举（见下）
    Err          error  // 仅失败结果（canceled / lock_timeout / error）非 nil
}

// Admin 重放结果枚举（AdminReplayEvent.Result 与后端管理面指标 result
// 属性取值）：
const (
    AdminReplayOK          = "ok"           // 重放生效（行原为 failed 且已重放）
    AdminReplayNoEffect    = "no_effect"    // 无效果（ok=false 且无错误）
    AdminReplayCanceled    = "canceled"     // 调用方取消
    AdminReplayLockTimeout = "lock_timeout" // 互斥锁等待超时（瞬时）
    AdminReplayError       = "error"        // 执行故障
)
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
| `go_event.append.supersede` | Counter | `event`（每次发布原子取代的同键待消费前驱行数，与 `AppendResult.Superseded` 同源；ADR 0014） |
| `go_event.fetch` / `.duration` | Counter + Histogram | `result`=ok/error/canceled |
| `go_event.claim` / `.duration` | Counter + Histogram | `result`=ok/error/canceled |
| `go_event.claim.granted` | Counter | `event`, `identity` |
| `go_event.claim.takeover` | Counter | `event`, `identity`（接管发放数，含终态接管；ADR 0011） |
| `go_event.claim.terminal_failed` | Counter | `event`, `identity`（认领侧直接置 failed 数，ADR 0011） |
| `go_event.lease.abandoned` | Counter | `event` |
| `go_event.consume` | Counter | `mode`, `event`, `result` |
| `go_event.handle.duration` | Histogram | `mode`, `event`, `result` |
| `go_event.heartbeat` / `complete` / `fail` | Counter | `result`=ok/error |
| `go_event.wake` | Counter | `result`=sent/dropped |
| `go_event.subscribe` | Counter | `event`, `mode`, `result`（仅启动后动态注册计数；Start 前注册不计入，ADR 0015） |

属性值仅枚举与标识符（`identity` 为消费者身份，多实例区分维度）；业务
payload（事件 ID、错误原文）不入属性。日志仅低频生命周期事件
（`runner started/stopped`、`stopping, lease released for takeover` 等），
错误经 `error.type` + `error` 字符串化上报。

后端（pgxc / postgrec）埋迁移生命周期与管理面写操作，发射点单一
（包级 `Migrate` / `Verify`，driver ensure 与 Admin 写方法同路径无重复计量）：

```go
func WithLogger(l *slog.Logger) Option          // 缺省 slog.Default()
func WithMeterProvider(mp metric.MeterProvider) Option // 缺省 otel.GetMeterProvider()
func WithScopeName(scope string) Option         // 缺省模块名
func WithObserver(o Observer) Option            // 缺省无观察者（当前仅 Admin 发射）

// 迁移指标：<scope>.migrate（Counter）+ <scope>.migrate.duration（Histogram），
// 属性 result=ok/canceled/lock_timeout/permission_denied/verify_conflict/
// permanent/transient 与 schema；Migrate 成功打 Info 日志
// 「<pkg>: migration applied」（pkg 为 pgxc / postgrec；属性 schema、mode、
// duration_seconds）。
//
// 管理面指标：<scope>.admin.replay（Counter）+ <scope>.admin.replay.duration
// （Histogram，result 维度；操作入口到返回全程耗时——含 ensure、锁等待与
// 事务执行），Counter 属性 result=ok/no_effect/
// canceled/lock_timeout/error 与 schema；每次重放打一行 slog 审计日志
// 「admin replay」（op、schema、event_id、consumer_type、result；失败加
// error.type + error）并发 AdminReplayEvent（ObserverV3 分发）。
```

### 2.10 Inspector（只读查询面，可选）

查询面与运行面分离（design.md §6/§9.2）：`Inspector` 不进入 `Driver`
契约，只读、不参与认领/完成路径；调用方按能力探测接入——对后端 Driver
做 `event.Inspector` 类型断言，或经后端 `NewInspector` 显式构造（与
`Driver` 共享连接与命名空间选项）。可选拓展接口 + 能力探测的模式见
[ADR 0005](adr/0005-backend-capability-boundary.md)。

```go
// Inspector 只读查询面（可选接口）：三类查询。结果仅在保留窗口内可见
// （超窗且无活跃租约的行被惰性清理删除后自然不可见，Inspector 不做
// 额外过滤——硬边界，design.md §5.2）。
type Inspector interface {
    // ClaimState 返回（事件 × 消费者身份）认领行的当前状态与重试计数。
    // ok 为 false 表示该身份对该事件无认领行——事件不存在、已超保留
    // 窗口被清理、或该身份尚未认领，三者即「待处理」态（该身份无认领
    // 行的行按 superseded 属性细分进 CountStates 的 Pending / Superseded
    // 两档）；查询故障经 error 返回，不得折叠为 ok=false。
    ClaimState(ctx context.Context, eventID, consumerType string) (ClaimState, bool, error)

    // CountStates 按消费者身份对某事件类型做状态分档计数：六档互斥且
    // 完备，合计等于该类型事件总数。分档口径与 Claim/Fail/Complete/
    // Append 的落库事实严格对齐：Pending = 该身份无认领行且未被取代
    // （「待处理」口径即此，而非「退避中」）；Superseded = 该身份无认领
    // 行且已被合并取代（事件行属性档，ADR 0014）；Retrying = 认领行
    // status='retrying'；Processing = status='active'（该身份落库持有
    // 租约，不做租约过期推断——接管由认领路径发生）；Failed / Done =
    // 终态，done 为归档档，不计入其余档。
    CountStates(ctx context.Context, typ, consumerType string) (StateCounts, error)

    // FailedEvents 返回 failed 终态事件的分页列表，排序与分页边界见
    // FailedQuery。
    FailedEvents(ctx context.Context, q FailedQuery) ([]FailedEvent, error)
}

// ClaimStatus 认领行的查询面状态：processing 对应落库列值 active
// （持有租约），其余取值与落库列值一致。
type ClaimStatus string

const (
    ClaimRetrying   ClaimStatus = "retrying"   // 已失败退避中，待重认领
    ClaimProcessing ClaimStatus = "processing" // 租约内处理中（落库 status='active'）
    ClaimDone       ClaimStatus = "done"       // 完成终态（幂等、不可逆）
    ClaimFailed     ClaimStatus = "failed"     // 失败终态（可经管理面 Admin 重放）
)

// ClaimState （事件 × 消费者身份）的当前查询面状态。Attempts 是已计入的
// 重试计数（claim_count 落库事实，ADR 0006）：显式 Fail 计一次、接管
// 认领计一次；与租约/信封的 Attempts（含本次认领，= 本值 + 1）相差
// 一次当前投递——processing / done 行的实际尝试次数为本值 + 1。
type ClaimState struct {
    Status   ClaimStatus
    Attempts int
}

// StateCounts 某事件类型在某消费者身份下的状态分档计数（口径见
// Inspector.CountStates）。
type StateCounts struct {
    Pending    int // 待处理：该身份无认领行且未被取代
    Superseded int // superseded：该身份无认领行且已被合并取代（ADR 0014）
    Retrying   int // 重试中：Fail 未达终态
    Processing int // processing：落库持有租约（status='active'）
    Failed     int // failed 终态
    Done       int // done 终态（归档档）
}

// FailedQuery failed 分页列表条件。Type / ConsumerType 空串表示不限
// （跨类型 / 跨身份列出全部 failed 行）。
//
// 分页语义（偏移分页，边界写死）：Offset < 0 视为 0；Limit <= 0 返回
// 空列表（nil）且不发起查询；首页 Offset=0 返回排序后的前 Limit 条；
// 末页剩余不足 Limit 条时返回剩余条（可少于 Limit）；越界（Offset 不
// 小于结果总数）返回空列表（非错误）。排序固定为 failed_at 降序（最新
// 在前），同一时刻按事件 ID 升序保证确定性。稳定性边界：偏移分页以
// 每次查询时刻的数据为准，无快照——跨页间隙发生的终态变化（新增
// failed、超窗清理、管理面重放）可能造成跨页重复或遗漏。
type FailedQuery struct {
    Type         string
    ConsumerType string
    Offset       int
    Limit        int
}

// FailedEvent failed 列表行：认领键（事件 × 消费者身份）+ 事件类型 +
// 失败摘要事实。LastError 是最近一次终态失败的错误摘要（清洗后文本），
// 空串表示未提供摘要——接管终态（只写 failed_at）与 Error 为空串的
// 终态 Fail 均落 NULL（ADR 0009）。FailedAt 是进入 failed 终态的时刻；
// 不变式「failed_at 非空 ⟺ 行处于 failed 终态」仅对 v2 加列后的写路径
// 成立（ADR 0009）：v2 是 ADD COLUMN、无回填，升级前已存在的 failed 行
// 该列为 NULL，列表中映射为零值 time.Time（升级边界）。
type FailedEvent struct {
    EventID      string
    Type         string
    ConsumerType string
    LastError    string
    FailedAt     time.Time
}
```

### 2.11 Admin（管理面写接口，可选）

管理面写操作与运行面分离（design.md §6）：`Admin` 不进入 `Driver` 契约，
**也不挂在后端 Driver 类型上**——与 Inspector 的类型断言探测不同，管理写
路径只能经后端 `NewAdmin` 显式构造（与 `Driver` 共享连接与命名空间选项），
「非管理面路径无法调用」由类型形态保证。鉴权与操作审计由使用方负责。
形态与重放语义见 [ADR 0010](adr/0010-admin-write-plane-and-replay.md)。

写路径约束（契约硬约束）：

- **持锁**：写操作在事务内持有与迁移执行相同的命名空间互斥锁
  （`postgresschema.LockKey(schema)` 的 advisory 键，键含 schema——不同
  命名空间互不阻塞），与迁移执行串行化；等待有界（try-lock + 退避，与
  迁移执行器同一实现），等待上限经后端选项 `WithAdminLockWait` 按实例
  配置（缺省同迁移锁 `postgresschema.LockWaitTimeout`），超时返回
  `ErrAdminLockTimeout`（瞬时错误）。
- **校验对象/行归属**：归属校验即单条条件更新（CAS）本身——以（事件 ×
  消费者身份）主键定位唯一认领行、以 `status='failed'` 限定可改写集合，
  命名空间由构造期固定并校验的 schema 限定；不满足即不归属（不命中、
  `ok=false`），不产生跨行 / 跨命名空间改写。
- **记录操作来源**：每次调用发射 otel 指标 `<scope>.admin.replay` 与
  `<scope>.admin.replay.duration`（§2.9）、slog 审计日志（操作、事件 ×
  身份、结果；不含事件 payload 与 last_error 摘要，失败结果附
  error.type / error 分类属性）与类型化 Observer 事件 `AdminReplayEvent`
  （经 `WithObserver` 注入、`ObserverV3` 分发）。观测均为旁路，失败不
  影响业务结果。
- **错误可判别**：`ErrAdminLockTimeout`（锁等待超时，瞬时）——管理写
  路径的同键锁等待覆盖两处、同判本哨兵：写事务内的互斥锁等待，与未预热
  实例首次操作时 ensure 撞上的迁移锁等待（统一改判，不留迁移面哨兵
  泄漏口）；其余迁移面哨兵（`ErrInvalidSchema`、
  `ErrMigrationPermissionDenied`、`*VerifyError` 等，经首次操作的迁移
  模式处理产生，原样上抛）；连接 / 驱动故障（原样包装，以 `error.type` /
  SQLSTATE 判别）。执行故障一律经 error 返回，不得折叠为 `ok=false`。
- **无自动 down、迁移侧无台账**：管理面只提供上行写操作，不提供任何
  down / 回滚入口；迁移侧无执行台账，无需 reset / stamp 入口，schema
  异常经迁移重跑或人工修复（ADR 0007）。

```go
// Admin 管理面写操作（可选接口）：仅处置认领消费的 failed 终态行——
// done 幂等不可逆、不重放；active / retrying 行不重放（自动重试路径仍在
// 正常运转）。重放窗口 = 保留窗口：超窗且无活跃租约的行已被惰性清理
// 删除，重放该键与「行不存在」同判别。
type Admin interface {
    // ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为
    // 可认领（手工重放，粒度与认领键一致）。仅 status='failed' 的行可
    // 重放，由单条条件更新（CAS）完成：并发重放 / 认领 / Fail 竞态下
    // 不改写非 failed 行——已被重新认领（接管）或已完成的行不受迟到的
    // 重放请求影响。
    //
    // 重放写入（全部字段一次置入）：status='retrying'、claim_count=0
    // （此后 Fail 的重试计数从合法范围重新开始，RetryLimit 语义不失效）；
    // lease_token 清空（空串）、lease_until 置为 now（原租约凭证失效）；
    // next_eligible_at=now（立即可认领，不做退避）；failed_at 清空、
    // last_error 保留（ADR 0009 口径：重放行与失败行经 failed_at 区分，
    // 摘要留作最近一次失败的排障历史）。
    //
    // 返回值（与 Inspector 的 ok 判别风格一致）：ok=true 表示行原为
    // failed 且已重放；ok=false（nil error）表示无效果——行不存在（事件
    // 未知、超保留窗口被清理、该身份尚无认领行）或行非 failed 终态
    // （done / active / retrying），判别方式统一，需要区分原因时先经
    // Inspector.ClaimState 查询；err 非 nil 为执行故障（连接、迁移等），
    // 不得折叠为 ok=false。
    ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (ok bool, err error)
}
```

### 2.12 事务性入队（outbox，可选；core 不定义接口）

outbox 让「业务事务提交」与「事件入队」原子生效：调用方在自己的业务事务
内追加事件，事件 INSERT 与业务写共享同一事务，随事务原子提交/回滚；不
需要事务内发布的场景可跳过本面（普通 Publish 语义不变）。语义与广播位置
边界的定稿见 [ADR 0012](adr/0012-outbox-transactional-append-and-broadcast-ordering.md)。

**core 不定义 `TransactionalAppender` 接口**（与 Inspector 的可选接口形态
不同）：事务性入队的第一个参数是调用方业务事务的载体——pgxc 为 `pgx.Tx`、
postgrec 为 `*sql.Tx`、inmem 为包内缓冲事务——core 接口只能以 `any` 承载，
丢失全部类型安全与自文档性。各后端提供具体类型 `TransactionalAppender`
与 `NewTransactionalAppender` 构造（与 Admin 同款：可选面、独立构造路径、
与 Driver 共享连接与命名空间选项、不经运行面类型获得——经本构造或组合
对象 Store 聚合），签名见 §3 与 §4。

语义边界（契约硬约束，eventtest.RunTransactionalAppendSuite 钉死）：

- **提交与回滚归调用方**：库不 begin、不 commit；`AppendTx` 返回成功仅
  表示 INSERT 已在调用方事务内执行，回滚后不产生事件行。
- **提交前对外不可见**：未提交事务内的事件对 Fetch / Claim / HeadPosition
  均不可见——基线锚点只锚已提交行；提交后经轮询/认领可见。
- **幂等与普通 Append 同口径**：同 ID 首写为准，不覆盖、不重投；事务内
  重复 ID 与跨事务重复 ID 同判。
- **合并投递同事务**（ADR 0014）：`RawEnvelope.CoalesceKey` 非空时，
  同键待消费前驱的取代与新行插入共用同一语句定义、随业务事务原子生效
  ——回滚则取代一并撤销，前驱照常待消费。`AppendTx` 不返回取代计数
  （与「不返回 position」同一边界：取代随提交才生效，提交前的计数与
  回滚后的事实可能矛盾）。
- **广播位置边界（ADR 0006 重审定稿，ADR 0012 §2）**：就近唤醒信号不
  覆盖事务内发布（提交后由下一个轮询周期拉取/认领）；并发提交的更大
  position 会把广播内存水位推过晚提交的行，该行对在途广播实例永久漏投
  ——「Start 后发布的事件必然可投递」仅对普通 Publish 路径成立（单语句
  独立提交，理论残余窗口可忽略、非绝对保证，ADR 0012）；认领
  消费不受影响（Claim 直接查库）。
- **不触发惰性清理**：清理 DELETE 不落入调用方事务（不扩大其锁面），
  机会性清理由普通 Append / Claim 等路径照常承担。
- **隔离级别沿用调用方事务**：`AppendTx` 仅执行单条幂等 INSERT（无读
  改写 CAS），不设置也不改变事务隔离级别——可重复读下 INSERT 语义不变
  （新行对同事务后续语句可见、对其他事务提交后可见），无需为入队提升
  隔离级别。
- **不发射观测**：库侧任何发射点都在提交前，而提交归调用方——「AppendTx
  成功」与「事务最终提交」可能被回滚打断，提交前发射会与落库事实矛盾
  （既有观测契约以与落库事实一致为准）；业务事务成败由调用方在自己的
  提交点计量。
- **position 语义**：同事务内按 INSERT 序分配，不承诺跨事务连续；回滚不
  回收（Postgres 序列不回收，缺口不进消费契约）；`AppendTx` 不返回
  position，调用方不得把事务内的位置信息当水位使用。

## 3. Postgres 后端签名（postgrec / pgxc）

两个 Postgres 后端共享同一套能力与选项语义，仅连接载体不同：`postgrec`
基于标准库 `database/sql`（`*sql.DB`），`pgxc` 基于 pgx v5 连接池
（`*pgxpool.Pool`）；选项与迁移函数（`Migrate`/`Verify`/`Statements`/
`SQL`/`Capabilities`）在各自 module 内同名定义。

```go
package postgrec // github.com/jninng/go_event/postgrec（标准库 database/sql）

// NewDriver 基于 *sql.DB 构造 event.Driver。缺省迁移模式 Create：首次操作
// 时持迁移锁幂等应用版本计划全量语句。WithMigrateMode 对 NewDriver /
// NewStore（内部 Driver 构造期）生效；显式传给包级 Migrate 返回
// ErrMigrateModeUnsupported。
func NewDriver(db *sql.DB, opts ...Option) event.Driver

// NewInspector 构造只读查询面 event.Inspector（§2.10）：与 NewDriver 共享
// 连接与命名空间选项（同一 schema、同一迁移模式语义——首次查询按模式
// 处理持久结构）。探测方式：对 NewDriver 返回值做 event.Inspector 类型
// 断言（本后端 Driver 同时实现该接口），或经本构造函数显式构造。
func NewInspector(db *sql.DB, opts ...Option) event.Inspector

// NewAdmin 构造管理面 event.Admin（§2.11）：与 NewDriver 共享连接与命名
// 空间选项（同一 schema、同一迁移模式语义）。管理写路径不经 Driver 类型
// 暴露——Admin 不经运行面类型获得：经本构造或组合对象 Store 聚合（同一
// 内部构造路径）；鉴权由使用方负责。写路径持与迁移执行共用的命名空间
// 互斥锁并发射管理面观测（§2.9/§2.11 约束）。
func NewAdmin(db *sql.DB, opts ...Option) event.Admin

// NewTransactionalAppender 构造事务性入队面（outbox 可选面，§2.12 语义、
// ADR 0012）：与 NewDriver 共享连接与命名空间选项。不经运行面类型获得
// ——经本构造或组合对象 Store 聚合（同一内部构造路径）；core 不定义
// outbox 接口（事务载体是后端具体类型 *sql.Tx）。
func NewTransactionalAppender(db *sql.DB, opts ...Option) *TransactionalAppender

// TransactionalAppender 事务性事件入队：AppendTx 把事件 INSERT 加入调用
// 方的业务事务（*sql.Tx），随事务原子提交/回滚；提交与回滚归调用方，
// 库不 begin、不 commit。幂等与普通 Append 同口径（同 ID 首写为准）；
// 提交前对外不可见（Fetch / Claim / HeadPosition 只锚已提交行）；不触发
// 惰性清理、不发射观测（语义见 §2.12 与 ADR 0012）。
type TransactionalAppender struct{ /* 不透明 */ }

// AppendTx 在调用方业务事务 tx 内追加事件。position 在 INSERT 时分配，
// 回滚不回收（Postgres 序列不回收，缺口不进消费契约）；调用方不得把
// 事务内的位置信息当水位使用。
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx *sql.Tx, e event.RawEnvelope) error

// Store 组合对象（便利构造，非能力面）：形态与语义同 pgxc.Store——一个
// 对象同时持有 Driver / Inspector / Admin / TransactionalAppender 各面，
// 选项解析一次、经同一份配置传导；内部只建一个 Driver 实例，各面行为与
// 独立构造完全等价。Store 不实现任何能力接口，构造不触发迁移。
type Store struct {
	Driver                event.Driver
	Inspector             event.Inspector
	Admin                 event.Admin
	TransactionalAppender *TransactionalAppender
}

// NewStore 构造组合对象 Store。
func NewStore(db *sql.DB, opts ...Option) *Store

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
// 相同（缺省 Create）；WithMigrateMode 对 NewDriver / NewStore（内部
// Driver 构造期）生效；显式传给包级 Migrate 返回 ErrMigrateModeUnsupported。
func NewDriver(pool *pgxpool.Pool, opts ...Option) event.Driver

// NewInspector 构造只读查询面 event.Inspector（§2.10）：与 NewDriver 共享
// 连接与命名空间选项。探测方式：对 NewDriver 返回值做 event.Inspector 类型
// 断言（本后端 Driver 同时实现该接口），或经本构造函数显式构造。
func NewInspector(pool *pgxpool.Pool, opts ...Option) event.Inspector

// NewAdmin 构造管理面 event.Admin（§2.11）：与 NewDriver 共享连接与命名
// 空间选项。管理写路径不经 Driver 类型暴露——Admin 不经运行面类型获得：
// 经本构造或组合对象 Store 聚合（同一内部构造路径）；鉴权由使用方负责。
// 写路径持与迁移执行共用的命名空间互斥锁并发射管理面观测（§2.9/§2.11
// 约束）。
func NewAdmin(pool *pgxpool.Pool, opts ...Option) event.Admin

// NewTransactionalAppender 构造事务性入队面（outbox 可选面，§2.12 语义、
// ADR 0012）：与 NewDriver 共享连接与命名空间选项。不经运行面类型获得
// ——经本构造或组合对象 Store 聚合（同一内部构造路径）；core 不定义
// outbox 接口（事务载体是后端具体类型 pgx.Tx）。
func NewTransactionalAppender(pool *pgxpool.Pool, opts ...Option) *TransactionalAppender

// TransactionalAppender 事务性事件入队：AppendTx 把事件 INSERT 加入调用
// 方的业务事务（pgx.Tx），随事务原子提交/回滚；提交与回滚归调用方，
// 库不 begin、不 commit。幂等与普通 Append 同口径（同 ID 首写为准）；
// 提交前对外不可见（Fetch / Claim / HeadPosition 只锚已提交行）；不触发
// 惰性清理、不发射观测（语义见 §2.12 与 ADR 0012）。
type TransactionalAppender struct{ /* 不透明 */ }

// AppendTx 在调用方业务事务 tx 内追加事件。position 在 INSERT 时分配，
// 回滚不回收（Postgres 序列不回收，缺口不进消费契约）；调用方不得把
// 事务内的位置信息当水位使用。
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx pgx.Tx, e event.RawEnvelope) error

// Store 组合对象（便利构造，非能力面）：一个对象同时持有 Driver /
// Inspector / Admin / TransactionalAppender 各面。选项与 NewDriver 一致且
// 解析一次、经同一份配置传导各面（观测共享同一 scope 与 logger /
// Observer）；内部只建一个 Driver 实例——Inspector 视图、Admin 与
// TransactionalAppender 复用同一实例（共享 ensure 状态与失败负缓存），
// 各面行为与独立构造完全等价。组合而非合并接口：Store 不实现任何能力
// 接口，core 也不定义 event.Store；构造不触发迁移（沿 NewDriver 现状，
// 首次操作按迁移模式处理持久结构）。
type Store struct {
	Driver                event.Driver
	Inspector             event.Inspector
	Admin                 event.Admin
	TransactionalAppender *TransactionalAppender
}

// NewStore 构造组合对象 Store。
func NewStore(pool *pgxpool.Pool, opts ...Option) *Store

// Wake 跨实例唤醒总线（可选面，仅 pgxc 提供，ADR 0013）：专用连接
// LISTEN go_event_wake_<schema>（短 schema 直用前缀 + 全名；前缀 + 全名
// 超 Postgres 标识上限 63 字节的 schema 走确定性哈希通道名——前缀 +
// schema 前 32 字节 + 全名哈希，恰 63 字节，两侧同源派生必然一致，任意
// 合法 schema 都有通道）。同 schema 任一实例经 Append / AppendTx
// 实际插入事件行即 NOTIFY（INSERT 经 CTE 挂在插入事实之后，仅实际插入
// 时发出——幂等冲突不重发；通道与 payload 只带 schema 名，不带事件内容；
// pg_notify 随事务提交可见，AppendTx 的信号在提交时刻发出）。收到通知
// 后向 Bind 注入的 LocalWake 注入 NotifyAll 信号——仅触发本实例已挂起的
// Fetch / 唤醒等待，读取与认领仍走后端存储（ADR 0003 边界不变），错过
// 的信号由周期 poll 兜底。LISTEN 需专用连接（经连接池 Acquire 持有一条
// 不归还）；断开按有界退避重连（100ms 起、封顶 30s）并重新 LISTEN。
// 构造即后台监听；非法 schema 或连接池为 nil 时监听
// 不启动（信号退化由轮询兜底）。延迟行的信号在插入时发出、不判断
// deliver_at（推送只回答「有新行」，消费侧空拉一趟由轮询兜底）。
type Wake struct{ /* 不透明 */ }

// NewWake 构造跨实例唤醒总线并开始后台监听。选项与 NewDriver 共享
// （本面消费 schema 与 logger）。
func NewWake(pool *pgxpool.Pool, opts ...Option) *Wake

// Bind 桥接本实例就近唤醒总线：通知到达即向 lw 注入 NotifyAll 信号。
// 可重复调用（后一次生效）；未桥接时信号静默丢弃（轮询兜底）。
func (w *Wake) Bind(lw *event.LocalWake)

// Close 停止监听并释放专用连接（幂等）：阻塞至监听循环退出。
func (w *Wake) Close()

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
// 不校验。该选项对 NewDriver / NewStore（内部 Driver 构造期）生效，显式
// 传给 Migrate 返回 ErrMigrateModeUnsupported。
func WithMigrateMode(m event.MigrateMode) Option

// WithObserver 注入类型化观测旁路（缺省无观察者）：当前仅管理面 Admin
// 发射观测事件（AdminReplayEvent，经 ObserverV3 分发）。
func WithObserver(o event.Observer) Option

// WithAdminLockWait 覆盖管理写互斥锁的有界等待上限（缺省同迁移锁
// postgresschema.LockWaitTimeout，仅 Admin 写路径生效）：运维按实例的
// 锁等待预算配置。非正值不阻塞——保留一次 try-lock 后立即返回
// ErrAdminLockTimeout（瞬时）。
func WithAdminLockWait(d time.Duration) Option
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
跨实例推送唤醒由 pgxc 原生提供（`Wake`，ADR 0013），postgrec 退化回
轮询（database/sql 无驱动无关的 LISTEN 接入，不做实现）。

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

// RunRetentionSuite 运行保留窗口惰性清理契约（design.md §5.2；保留窗口
// 自可投递时刻起算，ADR 0013）。
// newDriver 须为每个用例返回配置了保留窗口（≤ 2s，推荐 2s）的
// 全新 Driver 实例（清理簿记按 Driver 实例记忆，共享实例会被节流跳过）。
func RunRetentionSuite(t *testing.T, newDriver func(*testing.T) event.Driver)

// RunDelayedDeliverySuite 运行延迟事件契约（deliver_at 定时投递，
// design.md §9.2、ADR 0013）：零值立即可投递（既有行为回归）、过去时刻
// 等价立即、未到期行不满足 Fetch / Claim 下推条件、恰好到期边界（时钟
// 注入，含到时刻本身）、信封 DeliverAt 原样透传、延迟行与广播水位的
// 组合边界（水位越过即不再广播、认领不受影响）。
func RunDelayedDeliverySuite(t *testing.T, d event.Driver)

// RunCoalescingSuite 运行合并投递契约（同键最新一次消费，design.md §9.2、
// ADR 0014）：latest-wins 计数与双模式排除（Fetch / Claim 均只见最新行）、
// 广播水位越过与 HeadPosition 锚定、新消费者身份跳过、保守取代范围
// （活跃租约 / 退避 / 过期租约 / 终态前驱不取代，新事件照常排队）、
// 作用域 (Type, key) 与空键、ID 正交（重发旧 ID 纯 no-op 不杀新赢家）、
// 同 ID 幂等、被取代延迟行永不到期投递、信封透传、AppendTx 提交前不可见
// / 提交后取代 / 回滚零事实、Inspector superseded 分档、保留期照常清理。
// newDriver 须为每个用例返回配置了保留窗口（≤ 2s）的全新 Driver 实例
// （同 RunRetentionSuite 契约）；beginTx 须与传入 driver 同源绑定事务
// 会话（同 RunTransactionalAppendSuite 的 TxSession），任一为 nil Fatal
// 终止——核心语义全后端同形实现，必须全过本套件。
func RunCoalescingSuite(t *testing.T, newDriver func(*testing.T) event.Driver,
    beginTx func(*testing.T, event.Driver) TxSession)

// RunInspectorSuite 运行只读查询面 event.Inspector 的契约测试
// （design.md §6/§9.2）：三类查询与 Claim/Fail/Complete 落库事实比对。
// d 须实现 event.Inspector（inmem 原生实现；Postgres 后端的 Driver 同时
// 实现该接口），探测失败按 ClaimStateReader 先例 Fatal 终止——提供查询面
// 的后端必须全过本套件。
func RunInspectorSuite(t *testing.T, d event.Driver)

// RunAdminReplaySuite 运行管理面 Admin 重放的契约测试（design.md §5.3/
// §9.2、ADR 0010）：重放后可再认领并完成、重试计数归零与循环重放、
// done/active/retrying/未知键无效果且判别统一、last_error 保留与
// failed_at 清空（经 ClaimStateReader 断言）、并发重放恰好一次生效、
// 被接管后迟到重放不污染新属主行、互斥锁占用期间锁超时且行不变/
// 释放后重放生效、canceled 结果判别、观测事件结果与事件 × 身份一致。
// admin 须非空（管理写路径不经 Driver 类型暴露，只能由后端 NewAdmin
// 构造获得），且 admin 须实现 AdminExclusionLock（HoldExclusionLock：
// 从外部占用与 Admin 写互斥的命名空间锁）与 AdminObserverSetter
// （WithReplayObserver：返回挂接观测旁路的等价 Admin）两个注入接口
// ——互斥与操作来源记录是管理面契约约束，任一缺失即 Fatal；d 须实现
// ClaimStateReader。
func RunAdminReplaySuite(t *testing.T, d event.Driver, admin event.Admin)

// RunMigrationSuite 运行迁移契约测试。suite 本身不读环境变量；门控
// （如 TEST_POSTGRES_DSN）在具体后端的测试内完成。
func RunMigrationSuite(t *testing.T, b MigrationBackend)

// RunStoreSuite 运行组合对象三面同源契约（便利构造，非能力面）：
// d / insp / admin 须为同一 Store 的三面（后端 NewStore 构造传入），
// 任一缺失 Fatal 终止——提供组合对象的后端必须全过。
func RunStoreSuite(t *testing.T, d event.Driver, insp event.Inspector, admin event.Admin)

// RunTransactionalAppendSuite 运行 outbox 事务性入队契约（可选面，
// design.md §6 表格「事务入队（可选）」行、ADR 0012）：回滚零事件、
// 中途回滚连带先前 Append 一起丢弃（钉住「不逐条自治提交」）、提交前
// 对外不可见（隔离性与 HeadPosition 锚点）、提交后广播/认领可见、事务
// 内幂等（首写为准）、回滚 position 缺口不破坏消费、乱序边界（并发业务
// 事务交错提交下广播漏投、认领不受影响）、延迟事件经 AppendTx 可设置
// （提交后到期前不可见、到期后可见，ADR 0013）。d 与 beginTx 须同源（同一
// 后端、同一命名空间）；beginTx 为 nil Fatal 终止——提供事务性入队的
// 后端必须全过本套件。套件不绑定后端事务载体（pgxc 为 pgx.Tx、postgrec
// 为 *sql.Tx、inmem 为缓冲模拟），由装配函数打包成 TxSession。
func RunTransactionalAppendSuite(t *testing.T, d event.Driver, beginTx func(t *testing.T) TxSession)

// TxSession 事务性 Append 套件的事务载体适配：同一 TxSession 的三次
// 调用共享同一后端事务（装配处不得逐调用开新事务）；全部方法 Fatalf
// 风格，失败立即终止用例。
type TxSession interface {
    AppendTx(t *testing.T, e event.RawEnvelope) // 业务事务内追加事件
    Commit(t *testing.T)                        // 提交业务事务
    Rollback(t *testing.T)                      // 回滚业务事务
}

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

// NewInspector 构造进程内只读查询面 event.Inspector（§2.10）：与 New
// 共享同一实现（*Driver 同时实现 Inspector），查询口径与 Postgres 后端
// 一致——返回落库事实，不做租约过期推断。
func NewInspector(opts ...Option) event.Inspector

// NewAdmin 构造进程内管理面 event.Admin（§2.11）：与传入的 Driver（须为
// 本包 New 构造，与 NewInspector 共享同一底层实例）共享同一份内存状态，
// 写路径不挂 Driver 类型；传入其他实现的 Driver 视为装配错误（panic）。
// AdminOption：WithAdminObserver（观测旁路）、WithAdminLockWait（互斥锁
// 等待上限，契约套件缩短用）。
func NewAdmin(d event.Driver, opts ...AdminOption) event.Admin

// NewTransactionalAppender 构造进程内事务性入队面（outbox 可选面，§2.12
// 语义、ADR 0012）：与传入的 Driver（须为本包 New 构造）共享同一份内存
// 状态；传入其他实现的 Driver 视为装配错误（panic，与 NewAdmin 同义）。
// 进程内事务为最小缓冲模拟：BeginTx 开启 *Tx，AppendTx 在 Driver 锁内
// 分配 position 并缓冲（对外不可见），Commit 单次锁内原子应用、Rollback
// 丢弃；position 分配即消耗、回滚不回收（与 Postgres 序列对齐，ADR 0012）。
func NewTransactionalAppender(d event.Driver) *TransactionalAppender

// AppendTx / BeginTx / Tx 语义见 §2.12：事务控制归调用方（Commit /
// Rollback），已终结事务再操作返回错误；非本 appender 开启的事务拒绝。
type TransactionalAppender struct{ /* 不透明 */ }

func (ta *TransactionalAppender) BeginTx(ctx context.Context) (*Tx, error)
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx *Tx, e event.RawEnvelope) error

type Tx struct{ /* 不透明 */ }

func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error

// Store 组合对象（便利构造，非能力面）：一个对象同时持有 Driver /
// Inspector / Admin / TransactionalAppender 各面，共享同一份内存状态与
// 互斥锁。Admin 为缺省构造（无 Observer、缺省锁等待）；需要观测注入或
// 缩短锁等待的测试可经 NewAdmin(st.Driver, ...) 构造等价 Admin 替换
// （共享同一底层实例）。
type Store struct {
	Driver                event.Driver
	Inspector             event.Inspector
	Admin                 event.Admin
	TransactionalAppender *TransactionalAppender
}

// NewStore 构造组合对象 Store（选项与 New 一致）。
func NewStore(opts ...Option) *Store

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
| 接管计数与终态计数可见（§9.2、ADR 0011）           | `Claim` 返回 `ClaimResult{Leases, Takeovers, TerminalFailed}`（与落库事实一致）+ `<scope>.claim.takeover` / `.terminal_failed` 指标 + `ObserverV4.OnClaim`   |
| 心跳、最大处理时长、超时停续（§5.3、ADR 0006）      | `HeartbeatRequest` + `WithLease/WithHeartbeat/WithMaxProcessing`             |
| 迟到 Complete/Fail 不生效（CAS，ADR 0006） | `LeaseToken` + Complete/Fail CAS 不命中静默 nil                                   |
| 重试上限 + 递增退避（§5.3、ADR 0006）         | `FailRequest.RetryLimit/Backoff`（Driver 内原子完成）+ `WithRetryLimit/WithBackoff` |
| 幂等 Append 首写为准（§4）                 | `Append` 语义注释 + 契约测试                                                         |
| 元数据透传（§4）                          | `RawEnvelope.Metadata` + `WithMetadata`                                      |
| 就近唤醒仅信号（ADR 0003）                  | `LocalWake` + `WithLocalWake`（Publisher/Runner 双侧注入）                         |
| 消费者身份构造默认 + 订阅覆盖（§5.1）             | `WithIdentity` + `WithConsumerType`，皆缺返回 `ErrIdentityMissing`                |
| 订阅全生命周期可注册、启动后即生效（§5、ADR 0015） | 订阅入口在 Stop 前任意时刻可用；启动后注册即时装配，广播基线锚定于注册调用；Stop 后返回 `ErrRunnerStopped` |
| 构造器显式注入、无包级状态（§4）                  | 全部 `NewX(依赖, opts...)`；`Event[T]` 为无状态声明值                                    |
| 持久结构初始化按模式（§8、ADR 0007）             | `MigrateMode` + 后端 `WithMigrateMode`/`Migrate`/`Verify`；缺省 Create                     |
| 迁移能力声明用于契约测试与文档判定（§8、ADR 0007） | `MigrationCapabilities` + `FaultPoint` + `Capabilities()`                          |
| 校验报告结构化、错误可 `errors.As`（§8）          | `VerifyReport`/`VerifyIssue`/`VerifyProblem` + `*VerifyError`                        |
| 保留窗口惰性清理（§5.2、ADR 0006）            | `pgxc.WithRetention` / `postgrec.WithRetention`                              |
| 查询面按能力探测接入（§6、ADR 0005）          | `event.Inspector` + 后端 `NewInspector`（后端 Driver 同时实现，类型断言探测） |
