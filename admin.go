package event

import (
	"context"
	"time"
)

// Admin 管理面写操作（可选接口，不加入 Driver）：面向运维处置的写路径，
// 与只读查询面 Inspector 分离；鉴权由使用方负责（只应授予运维角色）。
// 能力探测（ADR 0005）：不进入 Driver 契约、不在后端 Driver 类型上——
// 经后端 NewAdmin 显式构造或经组合对象 Store 聚合（同一内部构造路径），
// 与各自 Driver 共享连接与命名空间选项。
//
// 硬边界：仅处置认领消费的 failed 终态行（done 幂等不可逆、不重放）；
// 重放窗口 = 保留窗口，写操作持与迁移相同的命名空间互斥锁（瞬时超时
// 返回 ErrAdminLockTimeout）。形态与重放语义见 docs/api.md §2.11 与
// docs/adr/0010-admin-write-plane-and-replay.md。
type Admin interface {
	// ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为
	// 可认领（手工重放）：单条条件更新（CAS）仅命中 status='failed'，
	// 并发重放 / 认领 / Fail 竞态下不改写非 failed 行；行归属校验即
	// CAS 本身（命名空间由构造期 schema 限定）。重放写入与返回值三分
	// 契约（ok / ok=false / err）见 docs/api.md §2.11 与 design.md
	// §5.3；执行故障经 error 返回，不得折叠为 ok=false。
	ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (ok bool, err error)
}
