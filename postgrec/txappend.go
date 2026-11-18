package postgrec

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jninng/go_event"
)

// appendInsertSQL 是事件 INSERT 的单一语句定义：普通 Append（自管连接，
// 独立提交）与事务性 AppendTx（加入调用方业务事务）共用同一份，避免第
// 二份拷贝漂移（幂等语义一致：同 ID 首写为准，design.md §4）。
//
// 本后端不承载跨实例唤醒信号（database/sql 无驱动无关的 LISTEN 接入，
// 退化轮询，ADR 0013 §4）；唤醒信号的 pg_notify 变体仅 pgxc 提供。
const appendInsertSQL = `
INSERT INTO %s.event (id, type, payload, metadata, occurred_at, deliver_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)
ON CONFLICT (id) DO NOTHING`

// deliverAtArg 把信封的 DeliverAt 归一为落库参数：零值存 NULL（立即可
// 投递），非零原样写入 deliver_at。
func deliverAtArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// TransactionalAppender 事务性事件入队（outbox 可选面，不进 Driver 契约，
// ADR 0012）：调用方在自己的业务事务（*sql.Tx）内调用 AppendTx，事件
// INSERT 与业务写共享同一事务，随事务原子提交/回滚；提交与回滚归调用方，
// 库不 begin、不 commit。形态与 Admin 同款（ADR 0010）——可选面、独立
// 构造路径、与 Driver 共享连接与命名空间选项、不经运行面类型获得（经本
// 构造或组合对象 Store 聚合，同一内部构造路径）；core 不定义 outbox 接口
// （事务载体是后端具体类型，any 参数形态丢失类型安全，ADR 0012 §1）。
//
// 语义边界（ADR 0012，写进契约）：
//   - 入口先按迁移模式处理持久结构（ensure，与 Admin 写路径一致，在调用
//     方事务外的池连接上执行）；失败上抛，业务事务由调用方回滚。
//   - 幂等与普通 Append 完全一致（同 ID 首写为准，不覆盖、不重投）。
//   - 不触发惰性清理：清理 DELETE 不落入调用方事务（不扩大其锁面），
//     机会性清理由普通 Append / Claim 等路径照常承担。
//   - 不发射观测：库侧任何发射点都在提交前，而提交归调用方——「AppendTx
//     成功」与「事务最终提交」可能被回滚打断，提交前发射会与落库事实矛盾
//     （既有观测契约以与落库事实一致为准）；业务事务成败由调用方在自己
//     的提交点计量。
//   - 不返回 position：提交前 position 不对外可见，调用方不得把事务内的
//     位置信息当水位使用；HeadPosition 只锚已提交行。
type TransactionalAppender struct {
	d *Driver
}

// NewTransactionalAppender 构造事务性入队面：与 NewDriver 共享连接与命名
// 空间选项（同一 schema、同一迁移模式语义——首次操作按模式处理持久结构）。
func NewTransactionalAppender(db *sql.DB, opts ...Option) *TransactionalAppender {
	return &TransactionalAppender{d: newDriver(db, opts...)}
}

// newTransactionalAppenderOn 以给定 Driver 构造事务性入队载体：NewStore
// 复用其唯一 Driver 实例（共享 ensure 状态与失败负缓存），与 newAdminOn
// 同构。
func newTransactionalAppenderOn(d *Driver) *TransactionalAppender {
	return &TransactionalAppender{d: d}
}

// AppendTx 在调用方业务事务 tx 内追加事件（不提交、不回滚——事务控制归
// 调用方）。事件在提交前对外不可见（Fetch / Claim / HeadPosition 均只锚
// 已提交行），提交后经轮询/认领可见——就近唤醒信号不覆盖事务内发布
// （提交前发信号只会让消费者空拉一趟），广播消费下可能被并发提交的更大
// position 越过水位而漏投（ADR 0012 §2 写死的投递边界，需可靠投递用认领
// 消费——Claim 直接查库，不受水位影响）。position 在 INSERT 时分配，
// 回滚不回收（Postgres 序列不回收，缺口不进入消费契约）。
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx *sql.Tx, e event.RawEnvelope) error {
	// 调用方取消：不发起写（pre-canceled ctx 显式预检，与 Admin 写路径同口径）。
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ta.d.ensure(ctx); err != nil {
		return err
	}
	meta, err := marshalMetadata(e.Metadata)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(appendInsertSQL, ta.d.schema),
		e.ID, e.Type, e.Payload, meta, e.OccurredAt, deliverAtArg(e.DeliverAt)); err != nil {
		return fmt.Errorf("postgrec: append tx: %w", err)
	}
	return nil
}
