# ADR 0013: 延迟事件与跨实例低延迟唤醒

- 状态：已接受
- 日期：2026-11-18

## 背景

生产化收尾两项：

1. **延迟事件**：`deliver_at` 定时投递——未到期行不满足 Fetch/Claim 下推
   条件；与广播水位组合的边界（延迟行被水位越过即不再投递）需写进文档；
   后端原生支持优先，其余按能力探测。
2. **低延迟唤醒**：同进程 `LocalWake` 已具备（ADR 0003）；本项指**跨实例
   推送**——支持原生推送的后端提供跨实例唤醒，未支持的后端退化轮询；
   仅信号、不承载投递保证（ADR 0003 的边界不变）。

前序相关口径：需求条件下推（ADR 0004——过滤必须发生在读取/认领时）、
能力分级框架（ADR 0005——非核心可选能力走可选拓展面 + 能力探测）、
广播位置语义（ADR 0012——水位只进不退，乱序边界已写为广播显式边界）。

## 决策

### 1. 延迟事件：信封字段 + Fetch/Claim 下推 + 保留期自可投递时刻起算

- **承载形态**：core 不新增接口——延迟事件是既有 Append / Fetch / Claim
  语义上的一个可选字段，非独立能力面（与 outbox 的「core 不定义接口」
  不同：本项连后端具体类型构造都不需要，全部后端同形实现）。事件信封
  增加可选投递时刻字段 `DeliverAt`（`RawEnvelope` 与 `Envelope` 同名
  字段，零值 = 立即可投递）；发布侧经 `WithDeliverAt(t)` 设置，事务性
  入队复用 `RawEnvelope.DeliverAt`——Publish 与 AppendTx 均可设置。
- **判据时钟**：Fetch 原无时钟入参，`FetchQuery` 增加 `Now`（由 core 下
  推 `time.Now()`，与 `ClaimQuery.Now` 同口径）；Claim 复用既有
  `ClaimQuery.Now`。下推条件统一为 `deliver_at IS NULL OR deliver_at <= now`
  （含到时刻本身）；零值 Now 时任何非零 deliver_at 行不可见（判据时钟未
  推进），零值 deliver_at 行（存量 NULL）不受影响。过滤在单条读取/认领
  操作内完成（ADR 0004），不取回后过滤。过去时刻等价立即可投递，不报错。
- **HeadPosition / position 不变**：deliver_at 不改 position 分配与基线
  锚点语义——延迟行照常占用 position 并可进基线（由此产生 §2 的水位
  边界）。
- **保留期组合口径（写死）**：**保留窗口自「可投递时刻」起算**——清理
  判据为 `COALESCE(deliver_at, occurred_at) < cutoff`（inmem 同构）。
  理由：核验既有清理 WHERE（以 occurred_at 判定）后确认，若沿用原判据，
  延迟长于保留窗口的事件会在「等待投递」期间被清理，延迟事件在
  deliver_at − retention 之外不可用——这不是可用性口径能接受的使用方
  错误，故选择自动起算而非截断或报错。零值行（存量 NULL）走
  occurred_at 分支，语义完全不变，无需数据回填；deliver_at 晚于保留
  窗口是合法用法（不截断、不报错），使用方需自行保证窗口覆盖投递后的
  消费路径。schema 影响：**版本计划追加 v3**——`event` 表加
  `deliver_at timestamptz` 可空无默认（一条 ALTER TABLE 一列一个期望
  对象，沿 v2 先例）；expand-only，对既有行与旧二进制无害（旧二进制不
  写该列 = NULL = 立即，行为不变）。
- **与广播水位的组合边界（写死）**：延迟行占用 position；到期前被下推
  条件过滤，故不推进水位；但其后发布的即时行会把水位推过延迟行的
  position——延迟行**到期后也不再有广播可见性**（`position > watermark`
  不成立），对所有在途广播实例永久漏投。该边界与 ADR 0012 的乱序边界
  同源（水位只进不退 + 行与水位推进序不一致），不做机制修复：广播投递
  本就是尽力而为（ADR 0006），「延迟事件 + 广播消费者」组合只适用于
  「漏掉可容忍」的通知类事件；需要定时必达用**认领消费**（Claim 无水位
  条件，到期后最迟下一认领轮次可见）。已否决的备选：水位感知缺口（为
  已广播 position 维护簿记）——破坏广播零簿记核心收益，与 ADR 0012
  §2(b) 同理由否决。
- **契约钉死**：eventtest 新增 `RunDelayedDeliverySuite`（零值立即、
  到期边界含到时刻、Fetch/Claim 到期前过滤、信封透传、水位边界用例）、
  `RunRetentionSuite` 增保留期组合用例（超窗未到期行保留、可投递时刻
  超窗行清理）、`RunTransactionalAppendSuite` 增 AppendTx 延迟用例、
  `RunSuite` 增 Publish 路径用例（`WithDeliverAt` 经 Publish 落库）。

### 2. 跨实例唤醒（pgxc LISTEN/NOTIFY）：仅信号，桥接 LocalWake

- **语义边界不变**（ADR 0003）：跨实例信号与同进程信号同质——仅触发本
  实例已挂起的 Fetch / 唤醒等待立即拉取/认领，读取与认领仍走后端存储；
  不承载投递保证，错过的信号（断连窗口、通道满、本实例无对应订阅）由
  周期 poll 兜底。
- **NOTIFY 侧（发布）**：pgxc 的事件 INSERT 经 CTE 把 `pg_notify` 挂在
  插入事实之后（普通 Append 与事务性 AppendTx 共用同一语句定义）——仅
  实际插入行时发出（幂等冲突不重发信号）；`pg_notify` 随所在事务提交才
  可见，事务性入队的信号在提交时刻发出，与「提交前发信号只会让消费者
  空拉一趟」（ADR 0012 §2(a)）天然相容。延迟行的信号同样在插入（提交）
  时发出、不判断 deliver_at——推送只回答「有新行」，不回答「有可投递
  行」，消费侧空拉一趟由轮询兜底。
- **payload 边界**：通道名 `go_event_wake_<schema>`（按 schema 隔离，
  超出 Postgres 通道标识上限 63 字节的 schema 以**确定性哈希通道名**
  适配——前缀 + schema 前 32 字节 + 全名 FNV-1a 64 位哈希十六进制，
  恰 63 字节；LISTEN 与 NOTIFY 两侧经同一函数派生必然一致，不存在退化
  路径，写路径永不因信号失败）；payload
  只带 schema 名、**不带事件内容**（不携带事件 ID/type/payload）——
  信号无路由语义（通道本身即命名空间隔离），接收侧向本实例全部订阅
  广播唤醒（低频场景空拉一趟成本可忽略，design.md §1）。因此 core 的
  `LocalWake` 增加导出桥接入口 `NotifyAll()`（向本实例全部已订阅类型
  发信号，语义与发布侧内部 `notify` 一致）。
- **LISTEN 侧（订阅）**：pgxc 提供 `Wake`（具体类型，可选面——与
  Driver 共享连接与命名空间选项，不经运行面类型获得）：`NewWake(pool,
  opts...)` 构造即后台监听，`Bind(lw *event.LocalWake)` 桥接（可重绑），
  `Close()` 释放。**连接生命周期**：LISTEN 需专用连接——经连接池
  Acquire 持有一条连接、不随 DML 归还复用（LISTEN 会话状态无法随池连接
  归还清空）；连接断开按有界退避重连（100ms 起、封顶 30s，与迁移失败
  负缓存同风格）并重新 LISTEN，连接生命周期以日志可见（`pgxc: wake
  listener connected` / `disconnected, reconnecting` / `connect failed,
  retrying` / `reconnect failed, retrying`——措辞区分断开、首连失败与
  重连失败）。构造期非法 schema / 连接池为 nil 时监听不
  启动（信号退化由轮询兜底），不 fail-fast 整个进程。
- **api 形态**：三个入口（`NewWake` / `Bind` / `Close`）+ core 一个
  桥接方法（`NotifyAll`），无新指标、无 Observer 事件、无新哨兵错误。
  使用形态：`NewRunner(drv, event.WithLocalWake(wake))` 的既有注入不变，
  Wake 只是信号源之一。

### 3. 观测：不加新指标（保持最小）

- 评估结论：信号到达本身不计量——其效果由既有 `<scope>.fetch` /
  `<scope>.claim` 轮次指标承载（唤醒加速的轮次与轮询轮次同路径同口径，
  区分二者需要新维度且无运维决策价值）；信号丢失不可观测（本就由轮询
  兜底，与同进程 LocalWake 信号口径一致——ADR 0008 的 `<scope>.wake`
  仅计量发布侧信号发射结果）。重连等连接生命周期走 `WithLogger` 日志。

### 4. postgrec：不做实现，退化轮询（能力边界表注明）

- 评估 database/sql 下的 LISTEN 成本：标准库无异步通知 API，LISTEN 需要
  驱动特定的专用连接管理（lib/pq 的 Listen 或 pgx/stdlib 的连接穿透），
  会把驱动特定性引入 postgrec——而后者的存在理由正是 database/sql
  便携性。决定：**不做实现**；需要跨实例唤醒用 pgxc，postgrec 退化
  轮询（既有 `WithPollInterval` 语义，无新代码）。inmem 无跨实例面
  （同进程 LocalWake 已覆盖）。

## 后果

- core 变更最小：`RawEnvelope`/`Envelope` 增 `DeliverAt`、
  `FetchQuery` 增 `Now`、`WithDeliverAt` 选项、`LocalWake.NotifyAll`；
  Driver 接口签名不变（仅 `FetchQuery` 结构体字段增加，v0.x 允许）。
- pgxc / postgrec 同步 v3 计划与 SQL 下推；清理判据改为可投递时刻；
  postgrec 无唤醒代码路径（能力边界表口径）。
- eventtest 套件扩展见 §1；迁移契约（`RunMigrationSuite` 与后端集成
  的 v2→v3 升级用例）钉死加列收敛与幂等。
- 文档同步：design.md §5.2/§6/§9.2、api.md §2.2/§2.3/§2.4/§2.6/§3、
  usage.md §4.3/§5.2/§8.3/§8.5/§11、根与后端 README 能力边界表。
- 已知限制（写进 usage）：延迟行的 NOTIFY 在插入时发出（消费侧可能
  空拉一趟）；跨实例信号通道不带类型路由（单实例多类型订阅会同时被
  唤醒）；pgxc `Wake` 长持一条池连接（连接池 MinSize 配置需留意）。
