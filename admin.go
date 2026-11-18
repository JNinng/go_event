package event

import (
	"context"
	"time"
)

// Admin 管理面写操作（可选接口，不加入 Driver）：面向运维处置的写路径，
// 与只读查询面 Inspector 分离——运行面 Driver、查询面 Inspector、管理面
// Admin 三个面各自独立（design.md §6），写操作不经任何运行面类型暴露。
// 鉴权由使用方负责：管理面对象只应授予运维角色。
//
// 能力探测（ADR 0005）：Admin 不进入 Driver 契约，且不在后端 Driver 类型
// 上（与 Inspector 的类型断言探测不同）——不经运行面类型获得：经后端的
// NewAdmin 显式构造，或经后端组合对象 Store 聚合（同一内部构造路径），
// 与各自 Driver 共享连接与命名空间选项。形态与重放语义见
// [ADR 0010](docs/adr/0010-admin-write-plane-and-replay.md)。
//
// 边界（硬边界，写进契约）：
//   - 仅处置认领消费的 failed 终态行：done 幂等不可逆、不重放；active /
//     retrying 行不重放（自动重试路径仍在正常运转）。
//   - 重放窗口 = 保留窗口（design.md §5.2）：超窗且无活跃租约的行已被惰性
//     清理删除，重放该键与「行不存在」同判别。
//   - 互斥：写操作持有与迁移执行相同的命名空间互斥锁（键含 schema，不同
//     命名空间互不阻塞），与迁移执行串行化；锁等待有界，超时返回
//     ErrAdminLockTimeout（瞬时错误）。
//   - 观测（操作来源记录）：每次调用发射类型化 Observer 事件
//     （ObserverV3.OnAdminReplay）、otel 指标（后端 scope 的
//     <scope>.admin.replay，result 属性）与 slog 记录（操作、事件 × 身份、
//     结果；不含事件 payload 与 last_error 摘要，失败结果附 error.type /
//     error 分类属性）。观测均为旁路，不改变业务语义。
type Admin interface {
	// ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为
	// 可认领（手工重放，粒度与认领键一致）。仅 status='failed' 的行可
	// 重放，由单条条件更新（CAS）完成：并发重放 / 认领 / Fail 竞态下
	// 不改写非 failed 行——已被重新认领（接管）或已完成的行不受迟到的
	// 重放请求影响。
	//
	// 归属校验即 CAS 本身：以（事件 × 消费者身份）主键定位唯一认领行、
	// 以 status='failed' 限定可改写集合，命名空间由构造期固定的 schema
	// 限定（不同 schema 的行不相交）——不满足即不归属，条件更新不命中
	// （ok=false），不产生跨行 / 跨命名空间改写。
	//
	// 重放写入（全部字段一次置入）：
	//   - status='retrying'、claim_count=0：重试计数归零，此后 Fail 的
	//     计数从合法范围重新开始（RetryLimit 语义不失效）；
	//   - lease_token 清空（空串）、lease_until 置为 now：原租约凭证
	//     失效，行不再被任何属主持有；
	//   - next_eligible_at=now：立即可认领（不做退避）；
	//   - failed_at 清空、last_error 保留（ADR 0009 口径：重放行与失败
	//     行经 failed_at 区分，摘要留作最近一次失败的排障历史，直到
	//     下一次终态失败覆盖）。
	//
	// 返回值（与 Inspector 的 ok 判别风格一致）：
	//   - ok=true：行原为 failed 且已重放；
	//   - ok=false（nil error）：无效果——行不存在（事件未知、超保留
	//     窗口被清理、该身份尚无认领行）或行非 failed 终态（done /
	//     active / retrying），判别方式统一；需要区分原因时先经
	//     Inspector.ClaimState 查询；
	//   - err 非 nil：执行故障，不得折叠为 ok=false。错误可判别：
	//     ErrAdminLockTimeout（互斥锁等待超时，瞬时）、迁移面哨兵
	//     （ErrInvalidSchema、ErrMigrationPermissionDenied、
	//     *VerifyError 等，经首次操作的模式处理产生）与连接 / 驱动
	//     故障（以 error.type 判别，原样包装不吞掉）。
	ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (ok bool, err error)
}
