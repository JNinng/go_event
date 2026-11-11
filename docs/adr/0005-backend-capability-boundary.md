# ADR 0005: 后端能力边界与 Kafka 排除

- 状态：已接受
- 日期：2026-11-11

## 背景

core 语义（两种消费方式、租约、重试）需要落到多个后端。各后端原生机
制差异大：有的语义近 1:1（NATS JetStream），有的可模拟但有持久性代价
（Redis），有的模型正交（Kafka）。需要统一的能力分级框架与候选后端取舍。

## 决策

### 能力分级框架

- 核心语义在各后端必须达到**「原生或模拟」档**——「不支持」即无法通过
  eventtest 全套契约测试，核心语义不存在该档位；各后端文档必须给出
  **能力边界表**，注明每项语义的实现档位。
- 非核心的可选能力（延迟事件、管理面查询、NOTIFY 推送唤醒等）走**可选拓展
  接口 + 能力探测**，与 outbox 同模式；使用方按探测结果决定启用与否。
- Driver 保持**拉形状**（Claim/Fetch 返回批次）：推送型后端经其拉式 API
  适配（JetStream Fetch、XREADGROUP 等）；真推送投递留作可选
  DeliverySource 拓展，不动基线。
- 任何新后端必须通过 eventtest 全套契约测试并给出能力边界表。

### 候选后端评估

- **NATS JetStream（首选）**：语义近 1:1 原生映射——AckWait≈租约、
  InProgress≈心跳、Ack≈done、maxDeliver+Term≈重试上限与终态、
  Nats-Msg-Id≈幂等 Append（服务器去重窗口有限，边界表注明）、Subject 按
  类型订阅≈需求过滤、DeliverAll≈新身份重放历史。终态为事件×身份粒度：
  新身份（新 durable consumer）的 DeliverAll 重放全部保留消息——含其他
  身份已 Ack 的 done 事件，与「新身份重放全部窗口内历史」语义一致，原生
  满足；同身份 Ack 持久化保证 done 不对该身份重放。边界：同名 durable
  consumer 删除重建会保留 Ack 状态、不重放，「身份重建即从头」须以新
  consumer 名模拟；无按消息的 done/failed 状态查询，「完成状态上游可查」
  需外挂（如 JetStream KV）；均边界表注明。
- **RocketMQ（候选，限 5.x）**：认领语义映射接近 1:1——Pop Consumer 组内
  单投递≈同类型并发互斥（at-least-once）、InvisibleDuration≈租约（过期重投≈接管）、
  ChangeInvisibleDuration≈心跳、Ack≈done、重试主题 + maxReconsumeTimes +
  DLQ≈重试上限与终态、位点重置≈新身份重放、消费组订阅多 Topic≈需求过滤、
  5.x 任意时长定时消息≈延迟事件。边界：必须 5.x（pop/续约/任意延时均为
  5.x 能力）；幂等 Append 不原生（唯一 ID 去重需消费端或外部存储）；
  官方 Go client（gRPC proxy 模式）成熟度一般。
- **Redis Stream**：全部可模拟——Consumer Group≈consumer_type、消息 idle
  时间≈租约（XAUTOCLAIM 接管）、XCLAIM≈心跳、XACK≈done、PEL 投递计数≈
  重试上限，Lua 补原子性。边界：持久性弱于 DB；done 后消息即出 PEL，
  「完成状态可查」需额外归档；数据丢失时保证降级（文档须写明）。
- **RabbitMQ**：quorum queue + delivery-limit + consumer timeout 可覆盖
  认领语义。
- **Kafka：排除**——无按消息 ack/租约/选择性重投，模拟需外部状态存储，违背
  后端自包含；其高吞吐分区流模型与本库语义正交（本库定位非高并发非实时，
  见 design.md §1）。

## 理由

- 档位分级（核心语义必须原生或模拟、可选能力按后端尽力）+ 能力探测让
  语义成为清晰契约，避免某后端的缺失能力静默破坏语义。
- Kafka 排除的根本原因是模型正交而非能力优劣：强行模拟需引入外部状态存储，
  破坏"后端自包含"约束，且其强项（高吞吐流）不是本库目标。

## 备选

- **只支持 Postgres**：与"后端可插拔"目标冲突。
- **接受 Kafka 并外挂状态存储**：复杂度与故障面剧增，且目标场景用不到
  Kafka 的吞吐优势，否决。

## 后果

- Postgres 后端先行（P0），NATS JetStream 是第二个后端的首选（P2）。
- 每个后端的 README/文档必须包含能力边界表，eventtest 通过情况与档位分级
  一一对应。
