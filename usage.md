# go_event 使用指南

本文从零开始介绍 go_event 的装配与使用：事件声明、发布、两种消费方式、后端配置、测试与运维注意事项。接口签名以 [docs/api.md](docs/api.md) 为准，语义事实源是 [docs/design.md](docs/design.md)。

## 目录

- [1. 安装](#1-安装)
- [2. 核心模型](#2-核心模型)
- [3. 装配](#3-装配)
- [4. 发布事件](#4-发布事件)
- [5. 广播消费](#5-广播消费)
- [6. 认领消费](#6-认领消费)
- [7. 多身份与混合订阅](#7-多身份与混合订阅)
- [8. 后端配置](#8-后端配置)
- [9. 测试](#9-测试)
- [10. 选项速查](#10-选项速查)
- [11. 运维注意事项](#11-运维注意事项)
- [12. 错误与排查](#12-错误与排查)

## 1. 安装

core 与后端是独立 module，按需引入：

```bash
go get github.com/jninng/go_event            # core（package event）
go get github.com/jninng/go_event/pgxc       # Postgres 后端，pgx v5
# 或
go get github.com/jninng/go_event/postgrec   # Postgres 后端，database/sql

go get github.com/jninng/go_event/eventtest  # 契约测试 / 进程内 Driver
```

仓库本地开发使用 `go.work` 串联各 module，无需替换路径。子 module 独立发版，版本 tag 形如 `pgxc/v0.1.0`。

对 Go 版本的要求以各 `go.mod` 为准。

## 2. 核心模型

### 2.1 事件声明

事件是**纯声明值**，不携带运行时状态：

```go
type DocUploaded struct{ DocID string }

// Name 是认领与路由的稳定标识；改名即破坏性变更。
var docUploaded = event.Declare[DocUploaded]("doc.uploaded")

// 无参事件
var cacheFlush = event.Declare[event.NoPayload]("cache.flush")
```

同一个 `Event[T]` 值可用于发布，也可用于广播或认领订阅。建议声明为包级变量，避免同一事件名散落多处。

### 2.2 信封

消费者拿到的事件信封：

```go
type Envelope[T any] struct {
    ID         string            // 发布侧生成的唯一 ID，幂等去重与追踪主键
    Type       string            // 即 Event.Name
    Payload    T                 // 强类型载荷
    Metadata   map[string]string // 原样透传，如 trace_id
    OccurredAt time.Time
}
```

- `ID` 建议作为 handler 的幂等键：认领消费是 at-least-once，同一事件可能被处理多次。
- `Metadata` 由后端原样存取，库不解释其语义。

### 2.3 Codec

payload 默认 JSON 编解码，可替换：

```go
type Codec interface {
    Encode(payload any) ([]byte, error)
    Decode(data []byte, into any) error
}
```

```go
pub := event.NewPublisher(drv, event.WithPublisherCodec(myCodec))
run := event.NewRunner(drv, event.WithRunnerCodec(myCodec))
```

发布侧与消费侧必须使用相互兼容的 codec。

### 2.4 Driver

`Driver` 是库与存储之间的唯一接口，只做单次原子操作：

```go
type Driver interface {
    Append(ctx context.Context, e RawEnvelope) error
    Fetch(ctx context.Context, q FetchQuery) ([]RawEnvelope, error)
    HeadPosition(ctx context.Context, typ string) (string, error)
    Claim(ctx context.Context, q ClaimQuery) ([]Lease, error)
    Heartbeat(ctx context.Context, h HeartbeatRequest) error
    Complete(ctx context.Context, c CompleteRequest) error
    Fail(ctx context.Context, f FailRequest) error
}
```

轮询、心跳、重试、并发与停机等编排全部在 core 的 `Runner` 中完成。使用方通常无需实现 `Driver`；需要接入新存储时见 [第 9 节](#9-测试)。

## 3. 装配

`Publisher` 与 `Runner` 都是具体类型，依赖经构造函数显式注入：

```go
pub := event.NewPublisher(drv,
    event.WithIDGenerator(func() string { return uuid.NewString() }),
    event.WithLocalWake(wake),
)

run := event.NewRunner(drv,
    event.WithIdentity("default"),          // 认领订阅的默认身份
    event.WithPollInterval(5*time.Second),  // 缺省 5s
    event.WithLocalWake(wake),
    event.WithLogger(slog.Default()),
)
```

- 一个 `Publisher` 可发布任意类型的事件。
- 一个 `Runner` 可注册多个订阅，混合承载广播与认领。
- **订阅必须在 `Runner.Start` 之前注册**；Start 之后再订阅返回 `event.ErrRunnerStarted`。
- `LocalWake` 是可选的同进程唤醒总线：同一个实例同时注入 `Publisher` 与 `Runner` 后，发布成功会立即唤醒本实例对应类型的 poller。它只是信号，不跳过存储，跨实例与信号丢失由周期轮询兜底。

生命周期：

```go
if err := run.Start(ctx); err != nil {
    return err
}
defer run.Stop(context.Background())
```

或使用便捷封装：

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := run.Run(ctx); err != nil { // Start 后阻塞，ctx 取消后 Stop
    slog.Error("runner", "err", err)
}
```

`Stop` 会停止拉取/认领并等待在途 handler 跑完；在途 handler 的 `ctx` 不因停机取消，`Stop(ctx)` 的 ctx 超时是调用方兜底。

## 4. 发布事件

### 4.1 有参事件

三种等价写法：

```go
// 事件侧糖
err := docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"})

// 包级泛型函数（Go 方法不支持类型参数）
err = event.Publish(ctx, pub, docUploaded, DocUploaded{DocID: "d-1"})

// 同一调用带发布选项
err = docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"},
    event.WithMetadata(map[string]string{"trace_id": traceID}),
)
```

### 4.2 无参事件

```go
// 事件侧糖统一形态
err := cacheFlush.Publish(ctx, pub, event.NoPayload{})

// 生产者侧便捷方法
err = pub.PublishNoPayload(ctx, cacheFlush)
```

### 4.3 发布语义

- **ID 与时间自动填充**：`ID` 由 `IDGenerator` 生成（缺省 `crypto/rand` 16 字节 hex），`OccurredAt` 取发布时刻 UTC。
- **幂等 Append**：重复发布同一 ID 视为成功，首写为准，不覆盖既有行、不触发重投。若需要“业务键去重”，应自行在发布侧生成稳定 ID。
- **元数据透传**：`WithMetadata` 合并进事件元数据，后端原样存取。
- **发布是单次 Append**：默认每次发布独立提交。业务事务内原子发布（outbox）属 P1 能力。
- 发布成功后若注入了 `LocalWake`，会向本进程对应类型的订阅发一次非阻塞唤醒信号。

## 5. 广播消费

### 5.1 示例

```go
err := docUploaded.SubscribeBroadcast(run,
    func(ctx context.Context, e event.Envelope[DocUploaded]) error {
        slog.Info("doc uploaded",
            "doc", e.Payload.DocID,
            "event_id", e.ID,
            "trace_id", e.Metadata["trace_id"],
        )
        return nil
    },
    event.WithConcurrency(8), // handler 并发度，缺省 1
    event.WithFetchLimit(100), // 每轮最多取 100 条，缺省 100
)
```

### 5.2 语义

- **发完即忘**：不承诺 at-least-once，不承诺保序；handler 返回错误或 panic 仅记日志并跳过，不重试、不阻塞、不设死信。
- **存储侧无簿记**：已读水位保存在实例内存中，按订阅在 `Fetch` 时下推，只进不退。
- **起点在 `Start`**：`Runner.Start` 内对每个广播订阅调用 `Driver.HeadPosition`，把水位锚定到该类型当前最大位置。**Start 返回之后发布的事件才会被投递**；停机期间与 Start 之前发布的事件不补投。
- **多实例各自全收**：每个活跃实例独立拉取并处理全部事件，实例间互不感知；实例数不影响语义。
- **只接受通用选项**：广播订阅只允许 `WithConcurrency` 与 `WithFetchLimit`；传入认领专属选项返回 `event.ErrNotClaimSubscription`。
- **基线失败不启动**：任一广播订阅的 `HeadPosition` 查询失败，`Start` 返回错误且整体不启动，修复后可重试。

广播消费适合通知、缓存失效、广播类副作用等“不需要补投与完成追踪”的场景。需要可靠投递时改用认领消费。

## 6. 认领消费

### 6.1 示例

```go
run := event.NewRunner(drv,
    event.WithIdentity("vectorizer"), // 认领订阅的默认身份
    event.WithPollInterval(5*time.Second),
)

err := vectorize.SubscribeClaim(run,
    func(ctx context.Context, e event.Envelope[VectorizeRequest]) error {
        // handler 必须幂等：重复投递不能产生二次副作用。
        // 推荐以 e.ID 作为幂等键。
        return vectorizeDoc(ctx, e.Payload.DocID, e.ID)
    },
    event.WithConsumerType("doc-vectorizer"), // 覆盖默认身份；省略则用 "vectorizer"
    event.WithConcurrency(4),
    event.WithLease(30*time.Second),         // 租约 30s
    event.WithHeartbeat(10*time.Second),     // 心跳 10s；缺省 lease/3
    event.WithMaxProcessing(10*time.Minute), // 超时停止续租
    event.WithRetryLimit(5),                 // 达上限置 failed
    event.WithBackoff(event.ExponentialBackoff(time.Second, time.Hour)),
    event.WithFetchLimit(10),
)
```

### 6.2 消费者身份

- 认领键是 `consumer_type`。身份来源二选一：订阅级 `WithConsumerType`，否则用 Runner 构造级 `WithIdentity`；两者皆缺返回 `event.ErrIdentityMissing`。
- 身份是消费者的事实身份：**新增或变更身份等于新消费者从头认领**保留窗口内的全部历史事件（含其他身份已 `done`/`failed` 的事件），不是断点续跑。
- 终态是（事件 × 身份）粒度：一个事件可被 N 个身份各消费一次。

### 6.3 租约与心跳

- 认领即抢锁：同一身份同一时刻至多一个活跃租约（Postgres 用 `FOR UPDATE SKIP LOCKED`）。
- 每个在途租约由独立 goroutine 在 `min(处理结束, 最大处理时长)` 内自动心跳续租。
- 超过最大处理时长停止续租，租约到期后由其他实例接管；挂死的 handler 不会被无限续保。
- 心跳瞬时错误（如数据库抖动）只记日志并在下个间隔重试，不停止续租。
- `Heartbeat` 配置为 `0`、负数或 `>= lease` 时，统一钳制为 `lease/3`。

### 6.4 成功与失败

- handler 返回 `nil`：调用 `Complete` 置 `done`，幂等且不可逆。
- handler 返回错误或 panic：调用 `Fail`，递增重试计数并按退避置为可再次认领；计数达 `RetryLimit` 时置 `failed` 终态，不再认领。
- 租约过期被接管也计一次失败（前任未 Fail 而失去租约）；否则每轮挂死的事件会被无限接管。
- 退避由 `Backoff.Next(attempt)` 计算，`attempt` 从 1 开始。内建实现：
  - `event.LinearBackoff(base, max)`：`base × attempt`，封顶 `max`。
  - `event.ExponentialBackoff(base, max)`：`base × 2^(attempt-1)`，封顶 `max`。
  - 缺省 `ExponentialBackoff(1s, 1h)`；可实现 `event.Backoff` 自定义。
- 迟到的 `Complete`/`Fail`（租约已过期或被接管）不生效：不会覆盖新属主状态，也不会递增计数。这是正常结果，Driver 返回 `nil`。

### 6.5 至少一次与幂等

认领消费整体是 at-least-once，不承诺恰好一次。租约互斥只保证“同一时刻至多一个实例在处理”，接管、网络重试等场景下同一事件仍可能被处理多次。**handler 幂等是消费方责任**，推荐以 `e.ID` 作为幂等键：

- 去重表；
- `upsert` 或条件更新；
- 天然幂等的状态机写法。

### 6.6 认领与拉取节奏

- 每个类型独立轮询 `Claim`，每轮限额 `WithFetchLimit`，防止单类型洪泛饿死其他类型。
- 认领订阅启动即执行首轮认领，随后按 `WithPollInterval` 轮询；注入 `LocalWake` 时可被同进程发布立即唤醒。
- 批次内事件顺序不承诺。

## 7. 多身份与混合订阅

一个 Runner 可以同时承载多个认领身份，也可以混合广播订阅：

```go
run := event.NewRunner(drv,
    event.WithIdentity("default"),
    event.WithPollInterval(5*time.Second),
)

// 身份 A
_ = vectorize.SubscribeClaim(run, vectorizeHandler,
    event.WithConsumerType("vectorizer"),
    event.WithConcurrency(4),
)

// 身份 B：与 A 独立认领、独立心跳、独立终态
_ = thumbnail.SubscribeClaim(run, thumbnailHandler,
    event.WithConsumerType("thumbnailer"),
    event.WithConcurrency(8),
)

// 同一 Runner 上的广播订阅
_ = docUploaded.SubscribeBroadcast(run, notifyHandler,
    event.WithConcurrency(2),
)
```

注意每个身份的保留窗口重放语义：新增身份会从窗口内历史开始消费，包括其他身份已处理过的事件。

## 8. 后端配置

### 8.1 pgxc（pgx v5）

```go
pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app")
if err != nil {
    return err
}
defer pool.Close()

// 可选：显式迁移。首次使用 Driver 时也会自动幂等迁移。
if err := pgxc.Migrate(ctx, pool,
    pgxc.WithSchema("public"),
); err != nil {
    return err
}

drv := pgxc.NewDriver(pool,
    pgxc.WithSchema("public"),                // 缺省 public
    pgxc.WithRetention(7*24*time.Hour),       // 缺省 7 天
)
```

### 8.2 postgrec（database/sql）

`postgrec` 不携带数据库驱动，需要自行注册（例如 pgx 的 `stdlib`）：

```go
import (
    "database/sql"

    _ "github.com/jackc/pgx/v5/stdlib" // 注册驱动名 "pgx"
)

db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/app")
if err != nil {
    return err
}
defer db.Close()

// 可选：显式迁移；首次使用时也会自动执行。
_ = postgrec.Migrate(ctx, db, postgrec.WithSchema("public"))

drv := postgrec.NewDriver(db,
    postgrec.WithSchema("public"),
    postgrec.WithRetention(7*24*time.Hour),
)
```

### 8.3 持久结构

两个 Postgres 后端的表结构一致：

- `event`：`id` 主键、`type`、`payload bytea`、`metadata jsonb`、`occurred_at`、`position bigserial`，并有 `(type, id)` 唯一约束与 `(type, position)` 索引。
- `event_claim`：主键 `(event_id, consumer_type)`，含 `lease_token`、`lease_until`、`claim_count`、`status`、`next_eligible_at`；`event_id` 外键级联删除。

建表与索引由 `Migrate` 幂等完成（`CREATE ... IF NOT EXISTS`）。`NewDriver` 首次使用时会自动执行迁移；若连接账号没有 DDL 权限，应先用具备权限的连接显式调用 `Migrate`。迁移失败不会被缓存，后续操作会重试。

### 8.4 保留窗口与惰性清理

- `WithRetention` 缺省 7 天。清理资格统一为：**超过保留窗口且无活跃租约的行，无论终态与否**；窗口以事件的 `OccurredAt` 判定。
- 清理是机会性的：随 `Append` 触发，并按 `max(retention/4, 50ms)` 做墙钟节流；清理失败静默，不影响主流程。
- 对认领消费，保留窗口是语义硬边界：`done` 行过窗即删，“完成状态可查”只在窗口内成立；窗口必须覆盖最坏重试路径，建议：

  ```text
  保留窗口 ≥ 重试上限 × (最大退避 + 最大处理时长) + 心跳间隔 + 时钟偏差余量
  ```

- 对广播消费，保留窗口仅是存储清理边界，没有额外投递语义。

### 8.5 inmem

`eventtest/inmem` 提供进程内 Driver，适合单元测试与示例：

```go
drv := inmem.New()                          // 缺省不清理
drv = inmem.New(inmem.WithRetention(time.Minute))
```

条件过滤在内存模拟，仅供测试使用。

## 9. 测试

### 9.1 用 inmem 测试业务逻辑

```go
func TestVectorizeFlow(t *testing.T) {
    drv := inmem.New()
    wake := event.NewLocalWake() // 发布后立即唤醒，避免等待轮询
    pub := event.NewPublisher(drv, event.WithLocalWake(wake))
    run := event.NewRunner(drv,
        event.WithIdentity("vectorizer"),
        event.WithLocalWake(wake),
    )

    got := make(chan string, 1)
    err := vectorize.SubscribeClaim(run, func(ctx context.Context, e event.Envelope[VectorizeRequest]) error {
        got <- e.Payload.DocID
        return nil
    })
    if err != nil {
        t.Fatal(err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    if err := run.Start(ctx); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = run.Stop(context.Background()) })

    if err := vectorize.Publish(ctx, pub, VectorizeRequest{DocID: "d-1"}); err != nil {
        t.Fatal(err)
    }

    select {
    case doc := <-got:
        if doc != "d-1" {
            t.Fatalf("got %q", doc)
        }
    case <-time.After(5 * time.Second):
        t.Fatal("timeout waiting for event")
    }
}
```

### 9.2 为新后端跑契约测试

实现 `event.Driver` 后，必须通过全套契约测试：

```go
func TestMyBackendContract(t *testing.T) {
    drv := newMyDriver(t)
    eventtest.RunSuite(t, drv)
}

func TestMyBackendRetention(t *testing.T) {
    eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
        // 每个用例必须是全新 Driver，且配置保留窗口（≤ 2s，推荐 2s）。
        return newMyDriverWithRetention(t, 2*time.Second)
    })
}
```

`RunRetentionSuite` 的 `newDriver` 不能复用实例：清理簿记按 Driver 实例记忆并有节流，共享实例会让部分用例被跳过。

### 9.3 Postgres 集成测试

两个后端模块的集成测试通过环境变量启用，未设置时自动跳过；需在各自 module 目录内运行（嵌套 module 不会被根目录的 `./...` 覆盖）：

```bash
(cd pgxc && TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/postgres' go test ./...)
(cd postgrec && TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/postgres' go test ./...)
```

测试会为每次运行创建独立 schema 并在结束时删除。

## 10. 选项速查

### 10.1 生产者

| 选项 | 作用 | 缺省 |
| --- | --- | --- |
| `WithIDGenerator(g)` | 事件 ID 生成器 | `crypto/rand` 16 字节 hex |
| `WithPublisherCodec(c)` | payload 编码 | JSON |
| `WithLocalWake(w)` | 发布成功后的同进程唤醒 | 无 |
| `WithMetadata(m)`（发布级） | 合并元数据 | 无 |

### 10.2 Runner

| 选项 | 作用 | 缺省 |
| --- | --- | --- |
| `WithIdentity(s)` | 认领订阅默认身份 | 无（认领订阅必须设置） |
| `WithPollInterval(d)` | 轮询间隔 | 5s |
| `WithRunnerCodec(c)` | payload 解码 | JSON |
| `WithLocalWake(w)` | 接收同进程唤醒 | 无 |
| `WithLogger(l)` | 日志 | `slog.Default()` |

### 10.3 订阅

| 选项 | 适用 | 作用 | 缺省 |
| --- | --- | --- | --- |
| `WithConcurrency(n)` | 两者 | handler 并发度 | 1 |
| `WithFetchLimit(n)` | 两者 | 每轮按类型取数上限 | 100 |
| `WithConsumerType(s)` | 认领 | 覆盖默认身份 | 使用 Runner 身份 |
| `WithLease(d)` | 认领 | 租约时长 | 30s |
| `WithHeartbeat(d)` | 认领 | 心跳间隔 | `lease/3` |
| `WithMaxProcessing(d)` | 认领 | 最大处理时长，超过停止续租 | 10min |
| `WithRetryLimit(n)` | 认领 | 失败/接管次数上限 | 5 |
| `WithBackoff(b)` | 认领 | 递增退避 | `ExponentialBackoff(1s, 1h)` |

广播订阅传入任一“仅认领”选项会返回 `event.ErrNotClaimSubscription`。

### 10.4 后端

| 选项 | 作用 | 缺省 |
| --- | --- | --- |
| `WithSchema(s)` | 表所在 schema | `public` |
| `WithRetention(d)` | 保留窗口 | 7 天 |

## 11. 运维注意事项

- **选型**：通知类用广播；需要补投、失败重试、完成追踪的任务用认领。
- **广播不补投**：Start 之前与停机期间发布的事件不会投递；重启从当前头部开始。关键业务不要依赖广播保证送达。
- **幂等**：认领消费一定按 at-least-once 设计 handler，以事件 ID 为幂等键。
- **重复处理是正常的**：慢 handler 被误接管、心跳丢失等场景会出现并发重复处理；库不在消费路径做去重。
- **保留窗口要够长**：尤其认领消费，窗口必须覆盖最坏重试路径，否则未完成事件会被清理而丢失。
- **数据库权限**：自动迁移需要 DDL 权限；生产环境可在发布流程中显式调用 `Migrate`，运行时账号只给 DML 权限。
- **停机**：`Stop` 等待在途 handler；handler 本身过长会拖住停机，给 `Stop` 传超时 ctx 作为兜底。
- **轮询开销**：默认 5s 轮询一次；低频场景可调大，需要低延迟时注入 `LocalWake`（仅同进程）或后续使用 LISTEN/NOTIFY（P1）。
- **消费者身份变更**：新增身份会重放窗口内全部历史事件；改名等同于新身份，不是断点续跑。
- **failed 事件**：当前版本没有管理面接口；P1 将提供 failed 查询与手工重放（仅 `failed` 可重放，`done` 不可逆）。

## 12. 错误与排查

### 12.1 哨兵错误

| 错误 | 触发条件 | 处理 |
| --- | --- | --- |
| `event.ErrRunnerStarted` | `Runner.Start` 之后注册订阅 | 把所有订阅移到 Start 之前 |
| `event.ErrNotClaimSubscription` | 广播订阅传入认领专属选项 | 移除认领选项，或改为认领订阅 |
| `event.ErrIdentityMissing` | 认领订阅既无 `WithConsumerType` 也无 `WithIdentity` | 补齐身份 |

### 12.2 常见现象

**广播订阅收不到事件**

1. 事件是否在 `Runner.Start` 返回之后发布？Start 之前的事件按设计跳过。
2. 发布与订阅的 `Event.Name` 是否完全一致？
3. 发布与消费是否指向同一个数据库/schema？
4. 跨实例场景是否只依赖 `LocalWake`？唤醒仅限同进程，跨实例依赖轮询，检查 `WithPollInterval`。

**认领订阅不处理事件**

1. 身份是否设置（`WithIdentity` 或 `WithConsumerType`）？
2. 事件是否已处于该身份的 `done`/`failed` 终态？终态不会再次认领。
3. 是否处于退避窗口？失败后要等 `next_eligible_at` 到期。
4. 数据库连接与 schema 是否正确？

**同一事件被处理多次**

认领消费是 at-least-once：接管、心跳丢失、处理超时都会导致重复投递。确认 handler 幂等，并以 `e.ID` 去重。

**事件丢失**

1. 广播：停机期间发布的事件不会补投。
2. 认领：事件滞留超过保留窗口且无活跃租约，会被惰性清理。按 [8.4](#84-保留窗口与惰性清理) 的公式加长窗口。
3. 检查是否是新身份：新身份只重放保留窗口内的历史。

**Start 返回基线错误**

广播基线通过 `Driver.HeadPosition` 同步确立；数据库不可用、权限不足或 schema 不存在都会导致 Start 失败。修复后重试 Start 即可，未启动的 Runner 不会留下半启动状态。

### 12.3 日志

`Runner` 使用注入的 `slog.Logger` 记录：

- 广播 handler 失败（跳过）；
- 认领 handler 失败与失败记录结果；
- 心跳失败（下个间隔重试）；
- 超过最大处理时长停止续租；
- 拉取/认领失败。

生产环境建议注入结构化 logger，并保留 `event_id`、`type`、`consumer` 字段用于排查。
