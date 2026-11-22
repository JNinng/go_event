# ADR 0015: 启动后动态注册订阅

- 状态：已接受
- 日期：2026-11-22

## 背景

订阅须在 `Runner.Start` 前注册完毕（design.md §5 旧口径「订阅在 Runner 启动
前注册完毕，启动后动态注册留作扩展」，[ADR 0002](0002-two-consumption-modes-and-lease.md)
同句），Start 后订阅一律返回 `ErrRunnerStarted`，消费拓扑在启动时刻冻结
——运行期才知道的消费方（按配置启用的订阅、装配完成后注册）无法接入。

前序相关口径：基线锚定（[ADR 0006](0006-broadcast-fire-and-forget-and-claim-retry.md)
——Start 内同步 `HeadPosition`，返回即基线确立）、需求条件下推与消费零簿记
（[ADR 0004](0004-demand-driven-pull.md)——水位在 core 内存、每订阅独立），
契约测试 `RunSuite` 此前钉死「Start 后注册拒绝」。

## 决策

### 1. 注册入口不变、语义扩展：全生命周期可注册，启动后即生效

- `SubscribeBroadcast` / `SubscribeClaim` 签名不变。未启动时照旧入表、由
  Start 统一装配并锚定基线；启动后注册即时装配生效（并发槽位、补认领
  信号、就近唤醒挂接、poller goroutine 与 Start 共用同一条装配路径）。
- 已否决的备选：新增带 ctx 的独立 Register API——注册面翻倍、与强类型糖
  入口重复；基线锚定查询的取消途径由 Runner 运行期 context 承担（§3），
  不为此改注册签名。

### 2. 认领起点：对齐 §5.1 新身份口径，零新机制

- 启动后注册的认领订阅即时参与认领轮询：从当前可认领行（保留窗口内，
  含其他身份已 done/failed 的——终态是（事件 × consumer\_type）粒度）开始
  认领，后续事件照常。认领订阅本无簿记、无水位，poll 循环启动即首轮
  认领天然实现，不引入任何新状态。

### 3. 广播起点：基线锚定时机随注册

- 启动后注册的广播订阅在**注册调用内**同步调用 `Driver.HeadPosition`
  锚定基线——**注册返回即基线确立**，只处理注册之后发布的事件；锚定
  失败注册以包装错误返回、订阅不生效、既有订阅不受影响，修复后可重试
  注册（与 Start「基线失败不启动」同构）。
- 锚定查询使用 Runner 运行期 context（Start 建立后存于 Runner 字段复用）：
  Stop 时未完成的注册随之中止，生命周期自洽；注册 API 无 ctx 参数，
  不为此改签名。
- 并发注册各得各自 HeadPosition 快照，遵循每订阅独立水位的既有语义
  （ADR 0004）；ADR 0012 的普通 Publish 路径承诺对动态注册同样成立
  （注册返回后发布的事件必然可投递，仅普通 Publish 路径）。

### 4. 错误面：移除 ErrRunnerStarted，新增 ErrRunnerStopped

- **`ErrRunnerStarted` 移除（破坏性，v0.x 允许）**：动态注册后「Start 后
  再订阅」不再是拒绝路径，保留永不返回的哨兵是误导。
- **新增 `ErrRunnerStopped`**：Stop 后注册一律拒绝；Start ctx 已取消、
  poller 已退出但未 Stop 的窗口同样拒绝（避免注册出永不消费的死订阅）。
  Runner 一次性契约不变。

### 5. Runner 保持一次性

- Stop 不复位 started、重复 Start 仍为无操作；可重启不在本项范围——
  动态注册已消除「为补注册而重启」的主要动机，可重启牵涉基线重锚、
  在途 handler 清理等一整面语义，另立待办。已否决：随本项放开可重启。

### 6. 观测与契约

- **指标**：core 新增 `<scope>.subscribe` 计数器（event + mode + result
  维度），仅计启动后动态注册（Start 前注册不计入——后者经 Start 日志的
  订阅总数可见）。Observer 不扩展（未要求）。
- **契约钉死**：eventtest `RunSuite` 移除钉死守卫的
  `PostStartSubscribe_RejectedErrRunnerStarted`，新增——广播动态注册只投
  注册后事件、认领动态注册重放存量（backlog）+ 新事件、基线失败拒绝且
  修复后可重试（Driver 装饰器注入，套件不读环境变量）、Stop 后注册拒绝
  `ErrRunnerStopped`、动态订阅挂接就近唤醒、动态订阅在途 handler 被 Stop
  优雅等待。

## 后果

- core 变更：Runner 增 `stopped` / `runCtx` 字段与注册守卫，订阅装配抽取
  为 Start 与动态注册共用路径；errors.go 换哨兵（破坏性）；
  `runnerMetrics` 增 subscribe 计数。**Driver 契约与三个后端零改动**
  （HeadPosition / Fetch / Claim 全部复用）。
- 契约套件见 §6；pgxc / postgrec / inmem 经各自 RunSuite 入口自动执行
  新用例。
- 文档同步：design.md §5 / §6 / §9、api.md §2.1 / §2.5 / §2.7 / §2.9 /
  §6 对照表、usage.md §3 / §5.2 / §12 与动态注册配方、根 README。
- 已知边界（写进 usage）：启动后注册的广播订阅不补投注册前事件（同
  Start 基线口径）；多订阅同类型（广播，或同身份认领）本就合法，动态
  注册不引入新竞争面；Start ctx 取消未 Stop 的窗口注册被拒绝，调用方应
  以 Stop 为停机边界。
