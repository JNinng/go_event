package postgrec

import (
	"database/sql"

	"github.com/jninng/go_event"
)

// Store 后端组合对象（便利构造，非能力面）：一个对象同时持有运行面
// Driver、只读查询面 Inspector、管理面 Admin 与事务性入队
// TransactionalAppender。各面共享同一连接与命名空间（同一 schema、同一
// 迁移模式语义），选项解析一次、经同一份配置传导，观测口径一致。
//
// 组合而非合并接口（design.md §6 三面分离）：Store 不实现任何能力接口，
// core 也不定义 event.Store 接口，各面语义与独立构造完全等价。Admin 与
// TransactionalAppender 是容器聚合——写路径不经 Driver 类型获得（对
// Driver 做相应类型断言不成立；ADR 0010 / 0012），鉴权由使用方负责。
//
// 构造不触发迁移：首次操作时按迁移模式处理持久结构。
type Store struct {
	// Driver 运行面：与 NewDriver 等价（Store 内唯一实例）。
	Driver event.Driver
	// Inspector 只读查询面：同一 Driver 实例的只读视图（*Driver 同时
	// 实现 event.Inspector）。
	Inspector event.Inspector
	// Admin 管理面写操作（当前仅 failed 手工重放，ADR 0010）。
	Admin event.Admin
	// TransactionalAppender 事务性入队（outbox 可选面，ADR 0012）：与
	// Driver 共享连接与命名空间；AppendTx 接收调用方的 *sql.Tx，事务
	// 控制归调用方。
	TransactionalAppender *TransactionalAppender
}

// NewStore 构造组合对象：内部只建一个 Driver 实例——Inspector 视图、
// Admin 与 TransactionalAppender 载体复用同一实例（共享 ensure 状态与
// 失败负缓存），避免调用方自行拼装时的重复迁移与观测口径漂移。选项与
// NewDriver 一致。
func NewStore(db *sql.DB, opts ...Option) *Store {
	cfg := newConfig(opts)
	d := newDriverConfig(db, cfg)
	return &Store{
		Driver:                d,
		Inspector:             d,
		Admin:                 newAdminOn(d, cfg),
		TransactionalAppender: newTransactionalAppenderOn(d),
	}
}
