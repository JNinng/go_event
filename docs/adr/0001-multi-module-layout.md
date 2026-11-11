# ADR 0001: 多 module 布局与独立发版

- 状态：已接受
- 日期：2026-11-11

## 背景

go_event 包含 core（契约与编排）与多个后端（Postgres 先行，后续 NATS、
Redis 等），以及契约测试套件 eventtest。需要决定仓库与依赖组织方式：

- 单 module 会导致使用方引入 core 时被迫携带全部后端及其数据库驱动依赖。
- 独立多仓库则割裂契约测试（eventtest 必须与 core、各后端同处演进）。

## 决策

采用**多 Go module 布局，单仓库**：

- `core`（仓库根）：只依赖 stdlib（slog），不携带任何数据库驱动。
  otel metric API 随 P1 观测能力落地时引入（生产可观测基本不可避免，
  仅依赖 API 而非实现）；P0 不引入未使用的依赖。
- `postgrec/`、`eventtest/` 等各自独立 go.mod，使用方按需引入后端依赖。
- 本地开发用 go.work（`use` 指令）串联各子 module；发布时打嵌套 tag
  （如 `postgrec/v0.1.0`），发布后从 go.work 移除本地引用。
- module path 为 `github.com/jninng/go_event`；子 module 为
  `github.com/jninng/go_event/postgrec`、`github.com/jninng/go_event/pgxc`、
  `github.com/jninng/go_event/eventtest`。

## 理由

- 后端依赖隔离是硬需求：目标环境可能仅 PostgreSQL 可用，引入 core 不应拖入
  NATS/Redis 客户端。
- 单仓库保证契约测试（eventtest）与各后端原子演进，符合"接口的语义就是这套
  测试"的定位。
- 嵌套 tag 是 Go module 生态对单仓库多 module 的标准发版方式。

## 备选

- **单 module**：依赖污染，否决。
- **多仓库**：契约测试与跨 module 重构成本高，否决。

## 后果

- 新增后端 = 新增子目录 + go.mod（`github.com/jninng/go_event/<backend>`），
  并通过 eventtest 全套契约测试。
- CI 需按 module 维度构建与测试。
