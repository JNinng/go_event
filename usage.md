# go_event 使用指南

本文从零开始介绍 go_event 的装配与使用：事件声明、发布、两种消费方式、后端配置、测试与运维注意事项。接口签名以 [docs/api.md](docs/api.md) 为准，语义事实源是 [docs/design.md](docs/design.md)。

## 目录

- [1. 安装](#1-安装)
- [快速开始（pgxc）](#快速开始pgxc)
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
- [13. 可观测（otel + slog）](#13-可观测otel--slog)

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

## 快速开始（pgxc）

以 pgx v5 后端为例，一份可直接运行的完整程序（发布 + 认领消费）：

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jninng/go_event"
	"github.com/jninng/go_event/pgxc"
)

// 事件在包级声明一次：Name 是认领与路由的稳定标识，改名即破坏性变更。
type OrderCreated struct{ OrderID string }

var orderCreated = event.Declare[OrderCreated]("order.created")

func main() {
	// 进程退出信号 → ctx 取消；Runner 以 ctx 生命周期消费。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// 连接由应用方建立（本库不携带数据库驱动）。
	pool, err := pgxpool.New(ctx, "postgresql://postgres:secret@localhost:5432/app?sslmode=disable")
	if err != nil {
		slog.Error("connect", "error", err)
		return
	}
	defer pool.Close()

	// Driver：缺省 Create 模式，首次操作自动持迁移锁建表（需 DDL 权限）。
	drv := pgxc.NewDriver(pool,
		pgxc.WithSchema("public"),
		pgxc.WithRetention(7*24*time.Hour),
	)

	// 发布：ID 自动生成，同 ID 重复发布幂等（首写为准）；事件侧糖与
	// event.Publish(ctx, pub, orderCreated, ...) 等价（见 §4.1）。
	pub := event.NewPublisher(drv)
	if err := orderCreated.Publish(ctx, pub, OrderCreated{OrderID: "o-1"},
		event.WithMetadata(map[string]string{"trace_id": "tr-1"})); err != nil {
		slog.Error("publish", "error", err)
		return
	}

	// 消费（认领：消费者身份 + 租约 + 心跳 + 失败重试）。订阅须在 Start 之前注册。
	run := event.NewRunner(drv, event.WithPollInterval(5*time.Second))
	if err := orderCreated.SubscribeClaim(run,
		func(_ context.Context, e event.Envelope[OrderCreated]) error {
			slog.Info("got order", "id", e.ID, "order", e.Payload.OrderID, "attempts", e.Attempts)
			return nil // 返回错误计一次失败，按退避自动重试至上限
		},
		event.WithConsumerType("order-worker"),
	); err != nil {
		slog.Error("subscribe", "error", err)
		return
	}
	if err := run.Start(ctx); err != nil {
		slog.Error("start", "error", err)
		return
	}

	<-ctx.Done() // 消费直至收到退出信号

	// 优雅停机：停止认领，等在途 handler 跑完。
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = run.Stop(stopCtx)
}
```

要点：

- **换后端只换构造那一行**：`Driver` 接口后端无关，`Publisher` / `Runner` / 订阅代码对其他后端原样适用。
- **迁移**：缺省 `Create` 在首次操作时自动建表；运行账号只有 DML 权限时改用 `Verify` / `None` 并由部署流程建表，见 [§8.4](#84-迁移模式与权限)。
- **广播消费**：把 `SubscribeClaim` 换成 `SubscribeBroadcast`（发完即忘、无需消费者身份），见 [§5](#5-广播消费)。
- **就近唤醒**：同一 `event.NewLocalWake()` 同时注入 `NewPublisher` 与 `NewRunner`，同进程发布可立即触发本实例拉取，见 [§3](#3-装配)。
- **可观测**：otel 指标与 slog 日志缺省跟随全局装配，不注入即静默；需要接入见 [§13](#13-可观测otel--slog)。

### 其他后端

| 后端 | 载体 | 参考 |
| --- | --- | --- |
| `postgrec` | 标准库 `database/sql`（连接与驱动注册由使用方完成，如 pgx/v5/stdlib） | [§8.2](#82-postgrecdatabase_sql)、[postgrec/README.md](postgrec/README.md) |
| `inmem` | 进程内实现，不碰真实存储 | [§8.6](#86-inmem)、[§9.1](#91-用-inmem-测试业务逻辑) |

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
    ID         string            // 发布侧生成或 WithEventID 显式指定的唯一 ID，幂等去重与追踪主键
    Type       string            // 即 Event.Name
    Payload    T                 // 强类型载荷
    Metadata   map[string]string // 原样透传，如 trace_id
    OccurredAt time.Time

    Attempts    int  // 本次投递的尝试序号（含本次）；广播恒为 0（见 §6.7）
    LastAttempt bool // 本次是否为最后一次尝试；广播恒为 false（见 §6.7）
}
```

- `ID` 建议作为 handler 的幂等键：认领消费是 at-least-once，同一事件可能被处理多次。
- `Metadata` 由后端原样存取，库不解释其语义。
- `Attempts` / `LastAttempt` 是 core 在认领派发前注入的运行时字段（Driver 不落库）；语义与边界见 [§6.7](#67-尝试次数attempts-与-lastattempt)。

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

- **ID 与时间自动填充**：`ID` 缺省由 `IDGenerator` 生成（缺省 `crypto/rand` 16 字节 hex），也可用 `WithEventID` 显式指定；`OccurredAt` 取发布时刻 UTC。
- **显式 ID 与业务键幂等**：`WithEventID` 显式指定 ID，优先于 `IDGenerator`，适合按业务键做发布幂等：

  ```go
  err := docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"},
      event.WithEventID("doc.uploaded:d-1"),
  )
  ```

  ID 是事件主键、**全局唯一**；重复发布同一 ID 首写为准，不校验 payload，新 payload 被丢弃（Metadata 与 OccurredAt 同样保留首写）。**跨事件类型复用同一 ID 不会报错**，而是静默视为同一事件、丢弃第二个 payload，因此业务键必须按类型加前缀（或与类型绑定）后才能当 ID 用。空串返回 `event.ErrEmptyEventID`；进程内锁不能跨实例去重，不要用它替代 ID 去重。
- **幂等 Append**：重复发布同一 ID 视为成功，首写为准，不覆盖既有行、不触发重投。若需要“业务键去重”，应使用 `WithEventID` 传入按类型加前缀的稳定业务键。
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
- 进入 `failed` 终态时，错误文本以清洗后的摘要落库（`last_error`，去除 Unicode 控制字符、截断 4 KiB、不含堆栈）并记录终态时刻（`failed_at`）；重试中不写。可直接查库排障（`SELECT ... FROM event_claim WHERE status = 'failed'`），见 [§8.3](#83-持久结构)。
- 租约过期被接管也计一次失败（前任未 Fail 而失去租约）；否则每轮挂死的事件会被无限接管。接管达上限直接置 `failed`，只写 `failed_at`（该路径没有本次处理错误，无摘要）。
- 退避由 `Backoff.Next(attempt)` 计算，`attempt` 从 1 开始。内建实现：
  - `event.LinearBackoff(base, max)`：`base × attempt`，封顶 `max`。
  - `event.ExponentialBackoff(base, max)`：`base × 2^(attempt-1)`，封顶 `max`。
  - 缺省 `ExponentialBackoff(1s, 1h)`；可实现 `event.Backoff` 自定义。
- 迟到的 `Complete`/`Fail`（租约已过期或被接管）不生效：不会覆盖新属主状态，也不会递增计数或写入摘要。这是正常结果，Driver 返回 `nil`。

### 6.5 至少一次与幂等

认领消费整体是 at-least-once，不承诺恰好一次。租约互斥只保证“同一时刻至多一个实例在处理”，接管、网络重试等场景下同一事件仍可能被处理多次。**handler 幂等是消费方责任**，推荐以 `e.ID` 作为幂等键：

- 去重表；
- `upsert` 或条件更新；
- 天然幂等的状态机写法。

### 6.6 认领与拉取节奏

- 每个类型独立轮询 `Claim`，认领的有效批大小为 `min(WithFetchLimit, 空闲并发槽位)`；所有槽位都被占用时不发起认领。`WithFetchLimit` 对认领是批次上限，不要求 ≤ `WithConcurrency`（缺省 100 > 1 是合法组合）。
- 认领返回即派发：派发不等待槽位，心跳随处理开始，因此不存在「已认领但尚未开始处理、租约不续租」的排队窗口（以 Driver 遵守 Claim 至多返回 Limit 条契约为前提）。
- 槽位释放即触发补认领，积压消化不依赖轮询间隔。
- 认领订阅启动即执行首轮认领，随后按 `WithPollInterval` 轮询；注入 `LocalWake` 时可被同进程发布立即唤醒。
- 批次内事件顺序不承诺。

### 6.7 尝试次数（Attempts 与 LastAttempt）

handler 可从信封读取本次投递的尝试序号，以及本次是否为最后一次尝试：

```go
_ = job.SubscribeClaim(run, func(ctx context.Context, e event.Envelope[Job]) error {
    slog.Info("job attempt", "id", e.ID, "attempts", e.Attempts, "last", e.LastAttempt)
    if e.LastAttempt {
        // 可用于观测/告警；不是终态回调。
    }
    return handle(ctx, e)
},
    event.WithRetryLimit(5), // LastAttempt 按此值计算
)
```

- `Attempts` 含本次：新认领为「已失败次数 + 1」，接管认领在先前基础上递增。
- `LastAttempt` 由 core 按订阅级 `RetryLimit` 计算：`Attempts >= RetryLimit` 时为 true（即本次是最后一次尝试）；用 `>=` 判定以防御多实例间的订阅配置差异。
- 边界：最后一次尝试的租约可能未被真正执行（进程崩溃/停机、或 handler 超 `MaxProcessing` 被接管）；此后接管会在认领时直接置 `failed`、不再补发租约，因此 handler 不得假设 `LastAttempt` 必然出现。它只用于观测与幂等辅助，不能替代业务终态处理（当前版本不提供终态回调）。
- 广播订阅恒为 `0` / `false`。

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

// 可选：部署/启动期显式迁移（与缺省 Create 模式同义，需 DDL 权限）。
if err := pgxc.Migrate(ctx, pool,
    pgxc.WithSchema("public"),
); err != nil {
    return err
}

drv := pgxc.NewDriver(pool,
    pgxc.WithSchema("public"),                // 缺省 public
    pgxc.WithRetention(7*24*time.Hour),       // 缺省 7 天
    // 运行账号只有 DML 权限、表已由部署流程建好时，改用只读自检：
    // pgxc.WithMigrateMode(event.MigrateVerify),
)
```

迁移模式只影响持久结构的初始化方式，详见 [§8.4](#84-迁移模式与权限)。

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

// 可选：部署/启动期显式迁移（与缺省 Create 模式同义，需 DDL 权限）；
// 缺省 Create 也会在首次操作时幂等应用版本计划。
_ = postgrec.Migrate(ctx, db, postgrec.WithSchema("public"))

drv := postgrec.NewDriver(db,
    postgrec.WithSchema("public"),
    postgrec.WithRetention(7*24*time.Hour),
    // 运行账号只有 DML 权限、表已由部署流程建好时，改用只读自检：
    // postgrec.WithMigrateMode(event.MigrateVerify),
)
```

迁移模式只影响持久结构的初始化方式，详见 [§8.4](#84-迁移模式与权限)。

### 8.3 持久结构

两个 Postgres 后端的表结构一致：

- `event`：`id` 主键、`type`、`payload bytea`、`metadata jsonb`、`occurred_at`、`position bigserial`，并有 `(type, id)` 唯一约束与 `(type, position)` 索引。
- `event_claim`：主键 `(event_id, consumer_type)`，含 `lease_token`、`lease_until`、`claim_count`、`status`、`next_eligible_at`，以及版本计划 v2 加列 `last_error`（错误摘要，可空 text）与 `failed_at`（进入 `failed` 终态的时刻，可空 timestamptz）；`event_id` 外键级联删除。摘要与终态时刻仅终态写入：`failed_at` 非空 ⟺ 行当前处于 `failed` 终态；`last_error` 是最近一次终态失败的清洗后摘要（截断 4 KiB、去控制字符、不含堆栈，空错误文本落 NULL）。

DDL 定义在共享的 `postgresschema` 方言包中（版本计划与派生语句、期望对象与共享执行器，无驱动依赖），两个后端使用同一份计划。首次操作是否建表、建到什么程度由迁移模式决定，见 [§8.4](#84-迁移模式与权限)。缺省模式 `Create` 会在首次操作时以构造 Driver 的连接（通常是运行账号）幂等应用版本计划全量语句。

只读查询面 `event.Inspector`（可选接口，pgxc / postgrec 均已提供）直接查询这两张表：状态与分档计数按 `event_claim` 行的 `status` 列与「无认领行」分档，failed 列表取 `status='failed'` 行并随行返回 `last_error` / `failed_at`——查询与落库事实严格对齐，见 [§11](#11-运维注意事项) 的「failed 事件查询」。`NewInspector` 首次查询按迁移模式处理持久结构：缺省 `Create` 模式下首次查询会持迁移锁执行 DDL，只读账号请配 `Verify` / `None`（表已由部署流程建好），见 [§8.4](#84-迁移模式与权限)。

管理写路径由独立 `event.Admin`（可选接口，pgxc / postgrec / inmem 均已提供）承载：`ReplayFailed` 把（事件 × 消费者身份）的 failed 终态行重置为可认领，写法与 `event_claim` 落库事实对齐（置 `retrying`、`claim_count=0`、清租约、`failed_at=NULL`、`last_error` 保留），见 [§11](#11-运维注意事项) 的「failed 手工重放（Admin）」。`NewAdmin` 与 `NewDriver` 共享连接与命名空间选项，写路径不经 Driver 类型暴露、只经该构造获得，鉴权由使用方负责。

### 8.4 迁移模式与权限

`event.MigrateMode` 有三种模式（没有 `Upgrade`：升级随部署隐式发生，见
[ADR 0007](docs/adr/0007-migration-mode-and-protocol.md)）：

| 模式 | Driver 首次操作时 | 所需连接权限 | 适用场景 |
| --- | --- | --- | --- |
| `Create`（缺省） | 持迁移锁幂等应用版本计划全量语句（基线与各版本）：建 schema、两表、约束与索引；升级随部署隐式发生 | DDL + DML | 部署/启动期用高权限连接初始化空库、幂等补齐缺失对象或随新二进制隐式升级 |
| `Verify` | 只读校验对象集合与计划期望（全量版本）一致，不执行 DDL；缺失表示待升级，定义冲突时首次操作返回 `*event.VerifyError` | DML + 系统目录只读 | 部署期已建表、运行账号只给 DML 时的启动自检 |
| `None` | 不建表也不校验，完全交给部署流程 | 仅 DML | schema 由外部流程严格管理，库不做检查 |

迁移执行为单一执行路径（无台账）：取锁 → 接管校验（对象集合与计划期望
比对：仅缺对象放行、定义冲突报差异错误）→ 整版单事务依次执行计划内全部
语句，任何失败整体回滚；中断后重跑即收敛。

- **无 DDL 权限的运行连接不能自动迁移**：`Create` 与显式 `Migrate` 同义，都以构造 Driver 的连接（通常是运行账号）执行 DDL。权限不足时返回 `event.ErrMigrationPermissionDenied` 并 fail-fast；正确做法是在部署期用高权限连接建表（调用 `Migrate` 或执行 `SQL` 导出的脚本），运行期改用 `Verify` 或 `None`。若后端锁能力为无（`LockNone`），`Create` 返回 `event.ErrMigrationUnsupported`；DDL 原子性为无是另一维度，同样只能 `Verify`/`None`。
- **失败缓存与重试有界**：`ensure` 的失败按 Driver 实例负缓存。瞬时错误（数据库不可用、`ErrMigrationLockTimeout`）按指数退避重试（100ms 起、封顶 30s），窗口内所有操作返回同一错误，不会在每次 Append/Claim 时重跑全部 DDL；永久配置错误（权限不足、对象定义冲突、非法 schema、后端不支持自动迁移）直接 fail-fast，不再重试。调用方取消不算迁移结论，不写入负缓存。
- **部署补建后需重启或重建 Driver**：永久配置错误在 Driver 生命周期内保持；`Verify` 首次通过后同样缓存「已检查」。表结构在运行期被改动时，只有重建 Driver 才会重新判定。
- **`Verify` 报告结构**：`event.VerifyReport` 含 `Issues []event.VerifyIssue`，每项含对象标识、对象类型、问题类别与处置说明（`Action`）；`Issues` 为空表示与计划期望一致。问题类别为 `missing`（对象缺失：待升级，处置为执行迁移；版本加列如 `last_error` / `failed_at` 缺失即属此类，可由迁移自动补齐）与 `outdated`（定义与计划期望不一致，不自动修复；缺失基线列属此类——迁移不修改既有表，须人工加列）；`too_new` / `failed` 为预留枚举，当前模型不产生。
- **`Statements` / `SQL` 导出物**：两者由同一版本计划派生。`Statements` 返回计划内全部语句清单（不含迁移锁）；`SQL` 返回可手工执行的整段脚本：以 `BEGIN` 开始，随后设 `lock_timeout` 并取迁移锁（锁语句位于全部 DDL 之前），随后依次输出计划内全部语句（基线 + 各版本，全部幂等），末尾 `COMMIT`。脚本整段可重放（中断重跑收敛）；手工执行 `SQL` 无需额外取锁；若按 `Statements` 逐条手工执行，必须先自行取锁并整段执行，否则并发首启会出现 DDL 竞态。
- **保留窗口不受影响**：迁移模式只决定持久结构的初始化方式，不改变 [§8.5](#85-保留窗口与惰性清理) 的保留期与惰性清理语义。

推荐组合：部署/启动期用高权限连接以 `Create` 完成初始化或随新二进制隐式升级；运行期用 `Verify` 做启动自检，或 `None` 完全交给部署流程——两者都不要求运行账号具备 DDL 权限。

### 8.5 保留窗口与惰性清理

- `WithRetention` 缺省 7 天。清理资格统一为：**超过保留窗口且无活跃租约的行，无论终态与否**；窗口以事件的 `OccurredAt` 判定。
- 清理是机会性的：随 `Append` 触发，并按 `max(retention/4, 50ms)` 做墙钟节流；清理失败静默，不影响主流程。
- 对认领消费，保留窗口是语义硬边界：`done` 行过窗即删，“完成状态可查”只在窗口内成立；窗口必须覆盖最坏重试路径，建议：

  ```text
  保留窗口 ≥ 重试上限 × (最大退避 + 最大处理时长) + 心跳间隔 + 时钟偏差余量
  ```

- 对广播消费，保留窗口仅是存储清理边界，没有额外投递语义。

### 8.6 inmem

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

实现 `event.Driver` 后，必须通过全套契约测试。Driver 须实现
`eventtest.ClaimStateReader`（套件据它断言 Fail 错误摘要与 `failed_at`
的落库事实，[§6.4](#64-成功与失败)）：`eventtest/inmem` 原生实现，
Postgres 后端在测试装配处以 SQL 查询 `event_claim` 行适配：

```go
func TestMyBackendContract(t *testing.T) {
    drv := newMyDriver(t)
    eventtest.RunSuite(t, drv) // drv 须满足 eventtest.ClaimStateReader
}

func TestMyBackendRetention(t *testing.T) {
    eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
        // 每个用例必须是全新 Driver，且配置保留窗口（≤ 2s，推荐 2s）。
        return newMyDriverWithRetention(t, 2*time.Second)
    })
}
```

`RunRetentionSuite` 的 `newDriver` 不能复用实例：清理簿记按 Driver 实例记忆并有节流，共享实例会让部分用例被跳过。

提供只读查询面的后端（pgxc / postgrec / inmem）加跑 `eventtest.RunInspectorSuite`：套件对传入的 Driver 做 `event.Inspector` 类型断言探测，失败即 Fatal——查询结果与 Claim/Fail/Complete 落库事实的比对是查询面契约的一部分。

提供管理面的后端（pgxc / postgrec / inmem）加跑 `eventtest.RunAdminReplaySuite(t, d, admin)`：管理写路径不经 Driver 类型暴露，`admin` 由后端 `NewAdmin` 构造传入（与 `d` 共享状态/连接），缺失即 Fatal——重放语义（仅 failed 可重放、计数归零、判别统一、`last_error` 保留 / `failed_at` 清空、并发 CAS 防护）是管理面契约的一部分。

迁移面另行通过 `eventtest.RunMigrationSuite` 验证，后端提供 `MigrationBackend` 适配实现（`NewDriver`/`Migrate`/`Verify`/`Statements`/`SQL`/`Capabilities`/`Prepare`/`Reset`/`InjectFault`/`Cleanup`）。进程内模拟 backend（`eventtest/simmigrate`）常驻，可在语句边界注入故障；真实数据库集成由 `TEST_POSTGRES_DSN` 门控。`InjectFault` 不支持某故障点时返回 `eventtest.ErrUnsupportedByBackend`。

### 9.3 Postgres 集成测试

两个后端模块的集成测试通过环境变量启用，未设置时自动跳过；需在各自 module 目录内运行（嵌套 module 不会被根目录的 `./...` 覆盖）：

```bash
(cd pgxc && TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/postgres' go test ./...)
(cd postgrec && TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/postgres' go test ./...)
```

测试会为每次运行创建独立 schema 并在结束时删除。仅 DML 权限连接的可诊断性另有 `TEST_DML_ONLY_DSN` 门控的可选用例，未设置时跳过。

## 10. 选项速查

### 10.1 生产者

| 选项 | 作用 | 缺省 |
| --- | --- | --- |
| `WithIDGenerator(g)` | 事件 ID 生成器 | `crypto/rand` 16 字节 hex |
| `WithEventID(id)`（发布级） | 显式指定事件 ID，优先于生成器；空串返回 `ErrEmptyEventID` | 使用生成器 |
| `WithPublisherCodec(c)` | payload 编码 | JSON |
| `WithLocalWake(w)` | 发布成功后的同进程唤醒 | 无 |
| `WithMetadata(m)`（发布级） | 合并元数据 | 无 |
| `WithMeterProvider(mp)` | 指标 provider（[§13](#13-可观测otel--slog)） | 构造期快照 `otel.GetMeterProvider()` |
| `WithScopeName(s)` | 指标 scope 与名前缀 | `go_event` |
| `WithObserver(o)` | 类型化观测旁路 | 无 |

### 10.2 Runner

| 选项 | 作用 | 缺省 |
| --- | --- | --- |
| `WithIdentity(s)` | 认领订阅默认身份 | 无（认领订阅必须设置） |
| `WithPollInterval(d)` | 轮询间隔 | 5s |
| `WithRunnerCodec(c)` | payload 解码 | JSON |
| `WithLocalWake(w)` | 接收同进程唤醒 | 无 |
| `WithLogger(l)` | 日志 | `slog.Default()` |
| `WithMeterProvider(mp)` | 指标 provider（[§13](#13-可观测otel--slog)） | 构造期快照 `otel.GetMeterProvider()` |
| `WithScopeName(s)` | 指标 scope 与名前缀 | `go_event` |
| `WithObserver(o)` | 类型化观测旁路 | 无 |

### 10.3 订阅

| 选项 | 适用 | 作用 | 缺省 |
| --- | --- | --- | --- |
| `WithConcurrency(n)` | 两者 | handler 并发度 | 1 |
| `WithFetchLimit(n)` | 两者 | 每轮按类型取数上限（认领侧另受空闲并发槽位封顶，见 [6.6](#66-认领与拉取节奏)） | 100 |
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
| `WithMigrateMode(m)` | 运行面迁移模式：`Create` / `Verify` / `None`（只对 `NewDriver` 有意义） | `Create` |
| `WithLogger(l)` | 迁移生命周期日志 | `slog.Default()` |
| `WithMeterProvider(mp)` | 迁移指标 provider（[§13](#13-可观测otel--slog)） | 构造期快照 `otel.GetMeterProvider()` |
| `WithScopeName(s)` | 迁移指标 scope 与名前缀 | 模块名（`pgxc` / `postgrec`） |

## 11. 运维注意事项

- **选型**：通知类用广播；需要补投、失败重试、完成追踪的任务用认领。
- **广播不补投**：Start 之前与停机期间发布的事件不会投递；重启从当前头部开始。关键业务不要依赖广播保证送达。
- **幂等**：认领消费一定按 at-least-once 设计 handler，以事件 ID 为幂等键。
- **显式 ID 去重是全局的**：ID 全局唯一，不同事件类型必须使用带类型前缀的业务键；跨类型复用同一 ID 会静默丢弃第二个 payload。去重边界由存储保证，进程内锁不能跨实例去重。
- **重复处理是正常的**：慢 handler 被误接管、心跳丢失等场景会出现并发重复处理；库不在消费路径做去重。
- **预取不会让租约空转过期**：认领按空闲并发槽位限额，认领后立即可派发并续租。但进程崩溃、停机（停止派发，未开始的租约到期后由其他实例接管）与 handler 超过 `MaxProcessing`（停止续租）仍会被接管，属 at-least-once 的正常表现，不是异常。
- **保留窗口要够长**：尤其认领消费，窗口必须覆盖最坏重试路径，否则未完成事件会被清理而丢失。
- **数据库权限**：自动迁移（`Create`）与显式 `Migrate` 同义，都需要 DDL 权限，且 `ensure` 在运行期以构造 Driver 的连接执行 DDL。**无 DDL 权限的运行连接不能自动迁移**：在部署期用高权限连接调用 `Migrate`（或执行 `SQL` 脚本）建表，运行期用 `WithMigrateMode(event.MigrateVerify)` 做启动自检，或 `None` 完全交给部署流程。
- **升级随部署隐式发生**：新二进制包含新版本语句时，`Create` 的首次操作（或显式 `Migrate`、重放 `SQL` 脚本）会幂等补齐缺失对象，没有独立的升级命令与停服窗口控制；升级语义要求每个版本的语句向后兼容，或停服迁移。
- **停机**：`Stop` 等待在途 handler；handler 本身过长会拖住停机，给 `Stop` 传超时 ctx 作为兜底。
- **轮询开销**：默认 5s 轮询一次；低频场景可调大，需要低延迟时注入 `LocalWake`（仅同进程）或后续使用 LISTEN/NOTIFY（P1）。
- **消费者身份变更**：新增身份会重放窗口内全部历史事件；改名等同于新身份，不是断点续跑。
- **failed 事件查询（Inspector）**：只读查询面 `event.Inspector` 已提供三类查询，按能力探测接入（对 Driver 做 `event.Inspector` 类型断言，或经后端 `NewInspector` 显式构造，与 Driver 共享连接与命名空间选项）：
  - 某（事件 × 消费者身份）的当前状态与重试计数：无认领行即「待处理」（ok=false），其余按落库 `status` 报 processing / retrying / done / failed（不做租约过期推断）；
  - 按消费者身份对事件类型的分档计数：**「待处理」= 该身份无认领行**（而非「退避中」），retrying / processing / failed / done 按认领行 `status` 分档，五档互斥且合计等于该类型事件总数；
  - failed 事件偏移分页列表：`last_error` / `failed_at` 随行返回，按 `failed_at` 降序（最新在前）、同刻按事件 ID 升序；Offset 越界返回空列表（非错误），`Limit <= 0` 返回空，偏移分页无快照——跨页间隙的终态变化可能造成重复或遗漏。**升级边界**：不变式「`failed_at` 非空 ⟺ failed」仅对 v2 加列后的写路径成立——v2 是 `ADD COLUMN`、无回填，升级前已存在的 failed 行该列为 NULL，列表中映射为零值。
  - 结果仅在**保留窗口内可见**（硬边界）：超窗行被惰性清理删除后自然查不到，Inspector 不做额外过滤。
  - `NewInspector` 首次查询按迁移模式处理持久结构：缺省 `Create` 模式下首次查询会持迁移锁执行 DDL——只读账号请配 `Verify` / `None`（表已由部署流程建好），见 [§8.4](#84-迁移模式与权限)。
  - 也可直接查库定位：`event_claim` 的 `last_error` / `failed_at`（`failed_at IS NOT NULL` 即当前 failed 行，见 [§6.4](#64-成功与失败)）。
- **failed 手工重放（Admin）**：管理写路径由独立 `event.Admin` 承载（pgxc / postgrec / inmem 均提供 `NewAdmin`，与 Driver 共享连接与命名空间选项；不进 Driver、不挂后端 Driver 类型——「非管理面路径无法调用」由类型形态保证），当前唯一操作 `ReplayFailed(ctx, eventID, consumerType, now)` 把（事件 × 消费者身份）的 failed 终态行重新置为可认领：
  - 仅 `status='failed'` 的行可重放：`done` 幂等不可逆、不重放；`active`（租约活跃）与 `retrying`（自动重试退避中）拒绝且不改动；行不存在（事件未知、超保留窗口被清理、该身份尚无认领行）与非 failed 行判别统一——返回 `ok=false` 且无错误，需要区分原因先经 `Inspector.ClaimState` 查询；执行故障经 `error` 返回。
  - 重放写入：置 `retrying`、`claim_count=0`（此后 Fail 的重试计数从合法范围重新开始）、清 `lease_token`、`lease_until` 置为重放时刻（原租约凭证失效）、`next_eligible_at=now`（立即可认领、不做退避）、`failed_at=NULL`、`last_error` 保留（排障历史，直到下一次终态失败覆盖）。
  - 并发防护是单条条件更新（CAS，仅命中 `status='failed'`）：并发重放恰好一次生效；已被重新认领（接管）或已完成的行不受迟到重放影响。
  - **重放窗口 = 保留窗口**：超窗行已被惰性清理删除，重放该键即 `ok=false`——发现 failed 后应在保留窗口内处置；窗口配置见 [§8.5](#85-保留窗口与惰性清理)。
  - 鉴权与操作审计由使用方负责：管理面对象只应授予运维角色。

## 12. 错误与排查

### 12.1 哨兵错误

| 错误 | 触发条件 | 处理 |
| --- | --- | --- |
| `event.ErrRunnerStarted` | `Runner.Start` 之后注册订阅 | 把所有订阅移到 Start 之前 |
| `event.ErrNotClaimSubscription` | 广播订阅传入认领专属选项 | 移除认领选项，或改为认领订阅 |
| `event.ErrIdentityMissing` | 认领订阅既无 `WithConsumerType` 也无 `WithIdentity` | 补齐身份 |
| `event.ErrEmptyEventID` | `WithEventID` 传入空串 | 传入非空业务键，或不使用该选项 |
| `event.ErrMigrationPermissionDenied` | `Create`/`Migrate` 的 DDL 权限不足 | 部署期用高权限连接建表；运行期改用 `Verify`/`None` |
| `event.ErrMigrationLockTimeout` | 等待迁移锁超时（瞬时错误） | 退避后自动重试；检查是否有长事务持有迁移锁 |
| `event.ErrMigrationUnsupported` | 锁能力为无（`LockNone`），`Create` 不支持自动迁移 | 改用 `Verify`/`None`，由部署流程建表 |
| `event.ErrMigrateModeUnsupported` | `WithMigrateMode` 传给 `Migrate`，或模式取值非法 | 该选项只传给 `NewDriver`；检查模式取值 |
| `event.ErrInvalidSchema` | schema 名非法（只允许 `^[A-Za-z_][A-Za-z0-9_]*$`） | 修正 schema 名；非法名不会进入任何 DML/DDL |

`Verify` 发现问题时返回类型化错误 `*event.VerifyError`，可用 `errors.As` 取回后读取 `Report`；它不是哨兵错误。

### 12.2 常见现象

**广播订阅收不到事件**

1. 事件是否在 `Runner.Start` 返回之后发布？Start 之前的事件按设计跳过。
2. 发布与订阅的 `Event.Name` 是否完全一致？
3. 发布与消费是否指向同一个数据库/schema？
4. 跨实例场景是否只依赖 `LocalWake`？唤醒仅限同进程，跨实例依赖轮询，检查 `WithPollInterval`。

**认领订阅不处理事件**

1. 身份是否设置（`WithIdentity` 或 `WithConsumerType`）？
2. 事件是否已处于该身份的 `done`/`failed` 终态？终态不会再次认领。`failed` 行可经 `event.Inspector` 查询定位原因（`LastError` / `FailedAt`），或直接查库 `last_error` / `failed_at`（[§6.4](#64-成功与失败)、[§11](#11-运维注意事项)）。
3. 是否处于退避窗口？失败后要等 `next_eligible_at` 到期。
4. 数据库连接与 schema 是否正确？

**同一事件被处理多次**

认领消费是 at-least-once：接管、心跳丢失、处理超时都会导致重复投递。确认 handler 幂等，并以 `e.ID` 去重。

**事件丢失**

1. 广播：停机期间发布的事件不会补投。
2. 认领：事件滞留超过保留窗口且无活跃租约，会被惰性清理。按 [8.5](#85-保留窗口与惰性清理) 的公式加长窗口。
3. 检查是否是新身份：新身份只重放保留窗口内的历史。

**Start 返回基线错误**

广播基线通过 `Driver.HeadPosition` 同步确立；数据库不可用、权限不足或 schema 不存在都会导致 Start 失败。修复后重试 Start 即可，未启动的 Runner 不会留下半启动状态。

**首次 Append/Claim 报迁移错误**

1. 缺省模式是 `Create`：检查构造 Driver 的连接是否有 DDL 权限。运行账号只有 DML 权限时，改用 `WithMigrateMode(event.MigrateVerify)` 或 `None`，并由部署流程建表。
2. `Verify` 报告 `missing`：按报告中的对象与处置补齐；报告 `outdated`：不自动修复，需人工对齐或走版本化迁移。
3. 权限不足、定义冲突等永久错误会 fail-fast 并在 Driver 生命周期内保持；修复后需重启或重建 Driver 才会重新判定（见 [§8.4](#84-迁移模式与权限)）。

### 12.3 日志

`Runner` 使用注入的 `slog.Logger` 记录：

- 广播 handler 失败（跳过）；
- 认领 handler 失败与失败记录结果；
- 心跳失败（下个间隔重试）；
- 超过最大处理时长停止续租；
- 拉取/认领失败。

生产环境建议注入结构化 logger，并保留 `event_id`、`type`、`consumer` 字段用于排查。

## 13. 可观测（otel + slog）

指标走 `go.opentelemetry.io/otel/metric`、日志走 `log/slog`，默认跟随全
局装配：未注入选项时构造期快照 `otel.GetMeterProvider()` / `slog.Default()`
——未配置 otel 时 provider 即 noop，静默零成本；应用侧 `otel.SetMeterProvider`
后，之后构造的组件自动接上。决策与口径分界见
[ADR 0008](docs/adr/0008-observability.md)，签名与指标清单见
[api.md §2.9](docs/api.md#29-可观测otel-指标--slog--observer)。

### 13.1 装配示例

```go
// 应用侧自由装配 exporter / handler，库不感知。
res := resource.NewWithAttributes("go_event")
reader := sdkmetric.NewManualReader() // 生产用 periodic reader + exporter
mp := sdkmetric.NewMeterProvider(
    sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
defer mp.Shutdown(context.Background())

publisher := event.NewPublisher(drv,
    event.WithMeterProvider(mp),            // 缺省跟随全局
    event.WithScopeName("go_event.orders"), // 部署期决策，勿随实例变化
)
runner := event.NewRunner(drv,
    event.WithMeterProvider(mp), event.WithLogger(slog.Default()),
    event.WithIdentity("orders-worker"),
)
```

### 13.2 指标口径

- **core**（scope 缺省 `go_event`）：`publish` / `fetch` / `claim` /
  `heartbeat` / `complete` / `fail` 计数与耗时、`claim.granted`、
  `lease.abandoned`、`consume` / `handle.duration`、`wake`——全部在 core
  调用边界计量，后端不重复埋同口径操作指标。
- **后端**（scope 缺省模块名）：仅迁移生命周期 `migrate` /
  `migrate.duration`，发射点在包级 `Migrate` / `Verify`（driver ensure 同
  路径，无重复计量）；`result` 区分
  `ok` / `canceled` / `lock_timeout` / `permission_denied` / `verify_conflict` /
  `permanent` / `transient`。
- 属性只含枚举与标识符（`event` / `mode` / `result` / `identity` /
  `schema`），事件 ID 与错误原文不入属性；Prometheus 导出侧自动做
  `.`→`_` 并按单位追加 `_total` / `_seconds`。

### 13.3 Observer 旁路

需要进程内联动（非指标场景）时注入 `Observer`：回调在调用方 goroutine 同
步执行、须微秒级返回，库内 recover panic；`ObserverV2` 增加停机放弃租约
与停机完成事件（非破坏性扩展）。

```go
// 实现 Observer（三个方法）即满足 V2；只需实现关心的额外事件，
// 其余留空方法体——库内 recover 保证旁路安全。
type slaObserver struct{}

func (slaObserver) OnPublish(e event.PublishEvent) {
    if e.Result != "ok" {
        slog.Warn("publish degraded", "event", e.Event, "result", e.Result)
    }
}
func (slaObserver) OnConsume(event.ConsumeEvent) {}
func (slaObserver) OnWake(event.WakeEvent)       {}
func (slaObserver) OnLeaseAbandoned(e event.LeaseEvent) {
    slog.Warn("lease abandoned on shutdown", "event", e.Event)
}
func (slaObserver) OnRunnerStopped(event.StopEvent) {}
```

### 13.4 测试断言

otel 同步仪表接口密封，指标断言用 `sdk/metric` 的 ManualReader 收集
`metricdata`；日志断言注入自定义 `slog.Handler`（本仓库测试即此写法，
见 `observability_test.go`）。
