# postgrec

go_event 的 Postgres 后端，基于标准库 `database/sql`（连接由使用方建立并
注册驱动，如 pgx/v5/stdlib）。能力与选项语义见
[docs/api.md](../docs/api.md)，语义事实源见 [docs/design.md](../docs/design.md)。

```go
db, _ := sql.Open("pgx", dsn)
_ = postgrec.Migrate(ctx, db)                      // 可选：显式迁移
drv := postgrec.NewDriver(db, postgrec.WithRetention(7*24*time.Hour))
```

## 能力边界表（ADR 0005）

核心语义全部为「原生」实现；eventtest 全套契约测试通过。

| 语义 | 档位 | 实现 |
| --- | --- | --- |
| 幂等 Append（首写为准） | 原生 | `event` 表 PRIMARY KEY (id) + `ON CONFLICT DO NOTHING` |
| 元数据透传 | 原生 | `jsonb` 列原样存取 |
| 广播 Fetch（类型过滤 + 水位 + 升序 + 限额） | 原生 | `WHERE type = $1 AND position > $2 ORDER BY position LIMIT $3`（bigserial 全序位置） |
| 广播基线锚点（HeadPosition） | 原生 | `SELECT COALESCE(MAX(position), 0) WHERE type = $1`（(type, position) 索引即时求值，单条语句快照；无行返回空串） |
| 认领互斥 / 接管 / SKIP LOCKED | 原生 | `FOR UPDATE SKIP LOCKED` + `event_claim` 唯一约束 + `ON CONFLICT DO UPDATE`（CAS 谓词） |
| 接管计入重试、达上限置 failed | 原生 | 认领事务内终态标记 + 认领递增计数 |
| 心跳 / 迟到 Complete / Fail 不生效（CAS） | 原生 | `lease_token` 条件更新，不命中静默返回 |
| 递增退避 | 原生 | Fail 事务内按 `Backoff.Next` 计算 `next_eligible_at` |
| 惰性清理 | 原生 | 超窗且无活跃租约（无论终态）行删除，`event_claim` 经 FK ON DELETE CASCADE 级联；墙钟节流，随 Append 机会性执行 |
| 持久结构初始化幂等 | 原生 | `CREATE SCHEMA/TABLE IF NOT EXISTS`，首次使用自动执行（`Migrate` 亦可显式调用） |

## 可选能力

| 能力 | 支持度 |
| --- | --- |
| 延迟事件（deliver_at） | P1 可选拓展，Postgres 原生可实现 |
| LISTEN/NOTIFY 推送唤醒 | 不支持（P1 由 pgxc 提供；本后端退化回轮询） |
| outbox（TransactionalAppender） | P1 可选拓展 |
