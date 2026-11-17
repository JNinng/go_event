package inmem

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jninng/go_event"
)

// TransactionalAppender 事务性事件入队（outbox 可选面，不进 Driver 契约，
// ADR 0012）：调用方在自己的业务事务内调用 AppendTx，事件与业务写共享
// 同一事务，随事务原子提交/回滚；提交与回滚归调用方。形态与 Admin 同款
// ——经 NewTransactionalAppender 构造或经组合对象 Store 聚合，不经运行面
// 类型获得。
//
// 进程内事务为最小模拟：AppendTx 在 Driver 锁内分配 position 并缓冲（对
// Fetch / Claim / HeadPosition 不可见——隔离性），Commit 在单次锁内原子
// 应用全部缓冲行，Rollback 丢弃缓冲；position 在 AppendTx 时分配、回滚不
// 回收（与 Postgres 序列不回收的行为对齐，ADR 0012 §5，缺口不进入契约）。
// 幂等与普通 Append 同口径（同 ID 首写为准）：AppendTx 时查已提交行与同
// 事务缓冲，Commit 时对交错提交的行重查首写为准。
type TransactionalAppender struct {
	d *Driver
}

// NewTransactionalAppender 构造事务性入队面：与传入的 Driver（须为本包
// New 构造）共享同一份内存状态。传入其他实现的 Driver 视为装配错误
// （panic，与 NewAdmin 同义——本包 Driver 为具体类型，无跨实现共享状态）。
func NewTransactionalAppender(d event.Driver) *TransactionalAppender {
	drv, ok := d.(*Driver)
	if !ok {
		panic("inmem: NewTransactionalAppender requires a driver constructed by this package")
	}
	return &TransactionalAppender{d: drv}
}

// BeginTx 开启业务事务（进程内缓冲模拟）。
func (ta *TransactionalAppender) BeginTx(context.Context) (*Tx, error) {
	return &Tx{d: ta.d}, nil
}

// AppendTx 在业务事务 tx 内追加事件（幂等：同 ID 首写为准）。事件在提交
// 前对外不可见，提交后经轮询/认领可见（就近唤醒信号不覆盖事务内发布，
// ADR 0012）。tx 为 nil、非本 appender 的事务或已终结的事务返回错误。
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx *Tx, e event.RawEnvelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("inmem: append tx: nil transaction")
	}
	if tx.d != ta.d {
		return errors.New("inmem: append tx: transaction belongs to another appender")
	}
	d := ta.d
	d.mu.Lock()
	defer d.mu.Unlock()
	if tx.done {
		return errors.New("inmem: append tx: transaction already finalized")
	}
	// position 在 AppendTx 时分配（缓冲行携带），回滚不回收——与 Postgres
	// 序列不回收对齐（ADR 0012 §5；缺口不进入消费契约）。
	d.position++
	ev := e
	ev.Position = strconv.FormatUint(d.position, 10)
	ev.Attempts = 0
	ev.LastAttempt = false
	// 幂等：同 ID 首写为准——已提交行存在或同事务已缓冲同 ID 均丢弃本次
	// 写入（缓冲行对内不可见，重复 ID 不产生第二行）。
	if _, ok := d.events[ev.ID]; ok {
		return nil
	}
	for _, b := range tx.buffered {
		if b.ID == ev.ID {
			return nil
		}
	}
	tx.buffered = append(tx.buffered, ev)
	return nil
}

// Tx 进程内业务事务（最小模拟）：缓冲事务性 Append，Commit 原子应用、
// Rollback 丢弃。状态迁移全部在 Driver 锁内完成；非本包 Driver 的事务
// 载体不可混用。
type Tx struct {
	d        *Driver
	buffered []event.RawEnvelope // 已分配 position 的缓冲行（保持 AppendTx 序）
	done     bool
}

// Commit 原子提交：单次锁内应用全部缓冲行（要么全部可见、要么全部不可
// 见），并按首写为准重查交错提交的行。重复 Commit / 已回滚的事务返回错误。
func (tx *Tx) Commit() error {
	d := tx.d
	d.mu.Lock()
	defer d.mu.Unlock()
	if tx.done {
		return errors.New("inmem: tx commit: transaction already finalized")
	}
	for _, e := range tx.buffered {
		if _, ok := d.events[e.ID]; ok {
			continue // 交错提交的行先落库：首写为准，不覆盖
		}
		d.events[e.ID] = &storedEvent{env: e, position: parsePosition(e.Position)}
	}
	tx.buffered = nil
	tx.done = true
	d.cleanupLocked(time.Now())
	return nil
}

// Rollback 回滚：丢弃全部缓冲行（position 已消耗，不回收）。已提交的事务
// 再回滚返回错误。
func (tx *Tx) Rollback() error {
	tx.d.mu.Lock()
	defer tx.d.mu.Unlock()
	if tx.done {
		return errors.New("inmem: tx rollback: transaction already finalized")
	}
	tx.buffered = nil
	tx.done = true
	return nil
}
