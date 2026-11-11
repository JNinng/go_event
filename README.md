# go_event

后端无关的事件生产/消费库（Go）。提供两种消费方式：

- **广播消费**：发完即忘，上游无感；消费端不在存储侧留下任何簿记。
- **认领消费**：消费者身份 + 租约 + 心跳 + 完成标记；失败自动重试且有上限。

两种消费方式与**强类型事件 / 无参事件**两个维度正交组合，支持分布式多实例与同进程就近唤醒。存储后端可插拔，内置两个 Postgres 实现（`pgxc` 基于 pgx v5，`postgrec` 基于 `database/sql`）。

> **定位**：面向低频事件的任务协作场景（长尾副作用异步化、完成状态追踪），不追求高吞吐流水线与毫秒级投递。选型与边界见 [docs/design.md](docs/design.md)。

## 特性

- **2×2 正交**：`Declare[T]` 声明强类型事件，`NoPayload` 表示无参事件；广播/认领共享同一 `Handler[T]` 签名。
- **广播发完即忘**：`Runner.Start` 内同步锚定基线，只处理启动后发布的事件；失败仅记日志跳过，不重试、不阻塞、不保序。
- **认领至少一次**：租约互斥、自动心跳续租、接管计入重试、递增退避、`done`/`failed` 终态；迟到的 `Complete`/`Fail` 不生效。
- **多身份**：一个进程可承载多个消费者身份，各身份独立认领；同一 `Runner` 可混合承载广播与认领订阅。
- **就近唤醒**：同进程发布触发本实例 poller 立即拉取；仅信号，不跳过存储，跨实例与信号丢失由轮询兜底。
- **幂等发布**：发布侧生成唯一事件 ID，重复 `Append` 同 ID 幂等（首写为准）；元数据块原样透传（如 `trace_id`）。
- **最小缝合点**：`Driver` 只做单次原子操作，全部编排（轮询、心跳、重试、并发、优雅停机）在 core 的 `Runner`。
- **多 module**：core 只依赖标准库，后端依赖按需引入；接口语义由 `eventtest` 契约测试约束。

## 两种消费方式对比

| 维度 | 广播消费 | 认领消费 |
| --- | --- | --- |
| 投递语义 | 尽力而为，不承诺 at-least-once | at-least-once，不承诺 exactly-once |
| 存储簿记 | 无；已读水位保存在实例内存 | `event_claim` 租约表 |
| 起点 | 订阅 `Start` 之后发布的事件 | 无主历史事件；新身份重放保留窗口内全部历史 |
| 失败处理 | 记日志跳过 | 退避重试，达上限置 `failed` |
| 消费者身份 | 不需要 | 必需（构造默认或订阅覆盖） |
| 多实例 | 每个实例各自全收 | 同身份同一时刻至多一个活跃租约 |
| 适用场景 | 通知、广播类副作用 | 任务、需要完成追踪与自动重试的副作用 |

## 安装

core 与后端是独立的 Go module，按需引入：

```bash
go get github.com/jninng/go_event            # core
go get github.com/jninng/go_event/pgxc       # Postgres（pgx v5）
# 或
go get github.com/jninng/go_event/postgrec   # Postgres（database/sql）

go get github.com/jninng/go_event/eventtest  # 用 inmem 做本地测试 / 编写新后端契约测试
```

## 快速开始

以下示例使用 `pgxc`，建表由后端在首次使用时自动幂等完成：

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

type DocUploaded struct{ DocID string }

// 事件声明：Name 是认领与路由的稳定标识，改名即破坏性变更。
var docUploaded = event.Declare[DocUploaded]("doc.uploaded")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app")
	if err != nil {
		slog.Error("connect", "err", err)
		return
	}
	defer pool.Close()

	drv := pgxc.NewDriver(pool, pgxc.WithRetention(7*24*time.Hour))
	wake := event.NewLocalWake() // 同进程发布 → 本实例 poller 立即拉取

	pub := event.NewPublisher(drv, event.WithLocalWake(wake))
	run := event.NewRunner(drv, event.WithLocalWake(wake))

	err = docUploaded.SubscribeBroadcast(run, func(ctx context.Context, e event.Envelope[DocUploaded]) error {
		slog.Info("doc uploaded", "doc", e.Payload.DocID,
			"trace_id", e.Metadata["trace_id"])
		return nil
	})
	if err != nil {
		slog.Error("subscribe", "err", err)
		return
	}

	if err := run.Start(ctx); err != nil {
		slog.Error("start", "err", err)
		return
	}
	defer run.Stop(context.Background())

	// 主流程发布即返回，上游无需感知消费方。
	if err := docUploaded.Publish(ctx, pub, DocUploaded{DocID: "d-1"}); err != nil {
		slog.Error("publish", "err", err)
	}

	slog.Info("consumer running; press Ctrl+C to exit")
	<-ctx.Done()
}
```

完整教程（认领消费、多身份、选项参考、测试与运维注意事项）见 [usage.md](usage.md)。

## 仓库布局

module path 为 `github.com/jninng/go_event`，子 module 独立发版（如 `pgxc/v0.1.0`）。

| 路径 | 内容 |
| --- | --- |
| `*.go`（仓库根） | core：事件声明、Publisher、Runner、Driver 接口、codec、就近唤醒（package `event`） |
| `pgxc/` | Postgres 后端，pgx v5 连接池 |
| `postgrec/` | Postgres 后端，标准库 `database/sql` |
| `eventtest/` | 契约测试套件；`eventtest/inmem` 为进程内 Driver |
| `docs/` | 设计文档、API 签名、ADR |

## 文档

- [使用指南](usage.md)：从安装到广播/认领消费、后端配置、测试与常见问题。
- [设计文档](docs/design.md)：能力面与语义的单一事实源。
- [API 签名](docs/api.md)：接口、结构体、选项与错误。
- [架构决策记录](docs/adr/)：多 module 布局、两种消费方式、就近唤醒、需求驱动拉取、后端能力边界、广播/认领语义。

## 开发与测试

仓库使用 `go.work` 串联各 module：

```bash
# core
go test ./...

# 各子 module 分别测试
(cd eventtest && go test ./...)
(cd pgxc && go test ./...)
(cd postgrec && go test ./...)
```

Postgres 集成测试需要真实数据库，通过 `TEST_POSTGRES_DSN` 启用（未设置时跳过）：

```bash
TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/postgres' go test ./...
```

新后端必须实现 `event.Driver` 并通过 `eventtest.RunSuite` 与 `eventtest.RunRetentionSuite` 全套契约测试。

## 状态与路线

- **P0（已实现）**：事件生产、广播消费、认领消费、就近唤醒、Postgres 两个后端、契约测试（覆盖两种消费方式与保留窗口清理）。
- **P1（规划）**：outbox、otel metric 观测、管理面（failed 查询与重放）、延迟事件、LISTEN/NOTIFY 推送唤醒。
- **P2（规划）**：NATS JetStream / RocketMQ / Redis Stream / RabbitMQ 后端、事件链、批量认领、限流背压。

详见 [docs/design.md §9](docs/design.md)。
