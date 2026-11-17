# ADR 0008: 可观测方案（otel 指标 + slog 日志 + 类型化 Observer）

- 状态：已接受
- 日期：2026-11-17
- 规范来源：基础库可观测规范（otel + slog），本 ADR 记录其在本库的落地方
  案与口径分界

## 背景

本库此前只有两类观测缝：Runner 的 slog 生命周期日志（缺省
`slog.Default()`，`WithLogger` 注入）与「以日志为同步点」的测试技巧；
发布、消费、唤醒丢弃、迁移成败均不可观测。TODO 中预留的方案是「可注入
`Metrics` 接口 + 实现方适配 otel」，其动机是不把 otel 依赖引入 core。

工程层面的可观测统一规范已经确定：日志走 stdlib `log/slog`、指标走
`go.opentelemetry.io/otel/metric`，库直接持有 otel 依赖并默认跟随全局装
配（未配置时 otel 全局 provider 即 noop），热路径零开销可埋点。该规范
取代「注入接口隔离 otel」的预留思路——隔离接口会把同一份字段定义在库与
适配层重复一遍，而 noop provider 已提供未配置时的零成本。

## 决策

### 1. 依赖面

- core（根模块）与两个后端依赖 `go.opentelemetry.io/otel` +
  `go.opentelemetry.io/otel/metric` + stdlib `log/slog`，不引入其他第三
  方依赖；`sdk/metric` 只出现在测试 import（ManualReader 断言）。
- `postgresschema` / `eventtest` / `inmem` 不引入可观测依赖。

### 2. 选项与快照语义

选项优先、全局兜底，构造期读取一次并固定：

- core：`WithMeterProvider` / `WithScopeName` / `WithObserver`（同时实现
  PublisherOption 与 RunnerOption，双面选项沿用 `WithLocalWake` 先例）；
  缺省 `otel.GetMeterProvider()` 与 scope `go_event`。`WithLogger` 已存在
  （缺省 `slog.Default()`），保持不变；Publisher 无日志点，不设该选项。
- 后端：`WithLogger` / `WithMeterProvider` / `WithScopeName`（func 型
  Option），缺省 `slog.Default()` / `otel.GetMeterProvider()` / 模块名。

快照后替换全局 provider 不追溯已建实例；`WithScopeName` 是部署期决策
（改名使仪表盘口径漂移），多实例区分走 `identity` 维度属性。

### 3. 口径分界（不重复计量）

- **core 负责全部 driver 操作与消费编排的指标**：`publish`、`fetch`、
  `claim`、`heartbeat`、`complete`、`fail` 的计数与耗时在 core 调用边界
  计量——后端不再重复埋同口径操作指标，两套数据无增量信息。
- **后端只负责迁移生命周期**：发射点单一，在包级 `Migrate` / `Verify`
  （driver ensure 经 `pgSchemaManager` 调用同一函数，ensure 路径与部署期
  显式调用全路径无重复）。迁移成功对 core 不可见（ensure 在后端内部），
  这是后端唯一的增量观测点。
- 指标命名 `<scope>.<动作>`（计数）与 `<scope>.<动作>.duration`（耗时，
  单位 `s`）；计数单位 `1`；属性只允许枚举与标识符（`event`、`mode`、
  `result`、`identity`、`schema`），业务 payload（事件 ID、错误原文）
  禁入属性值。

### 4. 类型化 Observer

- `Observer{OnPublish, OnConsume, OnWake}` + `ObserverV2`（嵌入扩展
  `OnLeaseAbandoned` / `OnRunnerStopped`），分发点类型断言，非破坏性演
  进；事件按值传递，字段仅值类型/string/error。
- 回调在调用方 goroutine 同步执行，库内 recover panic——观测是纯旁路，
  不改变业务执行语义。

### 5. 日志边界

日志只承载低频生命周期事件（启动、停机、停机放弃租约、迁移完成、心跳
放弃续租等既有点位）；热路径只打点不打日志。属性键对齐 OTel 语义约定：
`error.type` + `error`（字符串化），`attempts`、`schema`、
`duration_seconds` 带单位后缀。既有日志消息文本不变（契约测试以其为同
步点）。

## 后果

- core 首次引入第三方依赖（otel 两包）；未配置 otel 时 provider 即 noop，
  构造与打点零成本，调用方无感知。
- 已知口径限制：`Claim` 无法区分「接管」与「Fail 重试」（`Attempts` 混
  合两者，见 TODO），故无接管专属指标；停机放弃租约以
  `go_event.lease.abandoned` 计数（接管前兆）。
- `inmem` / `eventtest` 契约套件不感知可观测面；选项缺省语义不变，既有
  调用方零改动。
- 测试规范：指标断言用 `sdk/metric` ManualReader 收集 `metricdata`；日
  志断言用自定义 `slog.Handler`；`go test -race ./...` 必须通过。
