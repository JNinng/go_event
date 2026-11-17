package event

import (
	"context"
	"time"
)

// Admin 管理面写操作（可选接口，不加入 Driver）：面向运维处置的写路径，
// 与只读查询面 Inspector 分离——运行面 Driver、查询面 Inspector、管理面
// Admin 三个面各自独立（design.md §6），写操作不经任何运行面类型暴露。
// 鉴权与操作审计由使用方负责：管理面对象只应授予运维角色。
//
// 能力探测（ADR 0005）：Admin 不进入 Driver 契约，且不在后端 Driver 类型
// 上（与 Inspector 的类型断言探测不同）——调用方一律经后端的 NewAdmin 显式
// 构造，与各自 Driver 共享连接与命名空间选项。形态与重放语义见
// [ADR 0010](docs/adr/0010-admin-write-plane-and-replay.md)。
//
// 边界（硬边界，写进契约）：
//   - 仅处置认领消费的 failed 终态行：done 幂等不可逆、不重放；active /
//     retrying 行不重放（自动重试路径仍在正常运转）。
//   - 重放窗口 = 保留窗口（design.md §5.2）：超窗且无活跃租约的行已被惰性
//     清理删除，重放该键与「行不存在」同判别。
type Admin interface {
	// ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为
	// 可认领（手工重放，粒度与认领键一致）。仅 status='failed' 的行可
	// 重放，由单条条件更新（CAS）完成：并发重放 / 认领 / Fail 竞态下
	// 不改写非 failed 行——已被重新认领（接管）或已完成的行不受迟到的
	// 重放请求影响。
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
	//   - err 非 nil：执行故障（连接、迁移等），不得折叠为 ok=false。
	ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (ok bool, err error)
}
