package inmem

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jninng/go_event"
)

// TransactionalAppender 事务性事件入队（outbox 可选面，core 不定义接口，
// ADR 0012；语义边界见 docs/design.md §6「事务入队」行）：调用方在自己的
// 业务事务内调用 AppendTx，事件与业务写共享同一事务，随事务原子提交/
// 回滚；提交与回滚归调用方。形态与 Admin 同款——经 NewTransactionalAppender
// 构造或经组合对象 Store 聚合，不经运行面类型获得。
//
// 进程内事务为最小模拟：AppendTx 缓冲、Commit 原子应用、Rollback 丢弃，
// position 分配即消耗、回滚不回收（与 Postgres 序列不回收对齐，
// ADR 0012 §5）；幂等与普通 Append 同口径（同 ID 首写为准）。
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

// AppendTx 在业务事务 tx 内追加事件（幂等：同 ID 首写为准；CoalesceKey
// 非空时记录同键取代意图，ADR 0014：取代候选在调用时刻解析——已提交行
// 加本事务先前缓冲行，标记随事务提交生效、回滚丢弃。事件在提交前对外
// 不可见，提交后经轮询/认领可见（就近唤醒信号不覆盖事务内发布，
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
	// 写入（缓冲行对内不可见，重复 ID 不产生第二行；重复 Append 不触发
	// 取代，与普通 Append 同口径）。
	if _, ok := d.events[ev.ID]; ok {
		return nil
	}
	for _, b := range tx.buffered {
		if b.ID == ev.ID {
			return nil
		}
	}
	// 合并投递：调用时刻解析同键待消费前驱（已提交行 + 本事务先前缓冲行），
	// 标记随事务提交生效。
	if ev.CoalesceKey != "" {
		tx.supersede = append(tx.supersede, txSupersede{
			ids: d.resolveSupersedeLocked(ev.Type, ev.CoalesceKey, tx),
			at:  time.Now(),
		})
	}
	tx.buffered = append(tx.buffered, ev)
	return nil
}

// Tx 进程内业务事务（最小模拟）：缓冲事务性 Append 与同键取代意图，
// Commit 原子应用、Rollback 丢弃。状态迁移全部在 Driver 锁内完成；非本
// 包 Driver 的事务载体不可混用。
type Tx struct {
	d         *Driver
	buffered  []event.RawEnvelope // 已分配 position 的缓冲行（保持 AppendTx 序）
	supersede []txSupersede       // 与 buffered 对应的取代意图（AppendTx 时解析，提交时应用）
	done      bool
}

// txSupersede 是一次 AppendTx 的同键取代意图：候选前驱 ID 集与调用时刻
// （标记写入时刻的模拟）。
type txSupersede struct {
	ids []string
	at  time.Time
}

// resolveSupersedeLocked 解析同键待消费前驱（v1 保守口径，ADR 0014）：
// 同 (type, key)、未被取代、无任何认领行——已提交行加上本事务先前缓冲的
// 同键行（同事务可见自身未提交写，与 SQL 后端一致）。
func (d *Driver) resolveSupersedeLocked(typ, key string, tx *Tx) []string {
	var ids []string
	for id, ev := range d.events {
		if ev.env.Type != typ || ev.env.CoalesceKey != key {
			continue
		}
		if !ev.supersededAt.IsZero() || d.hasClaimLocked(id) {
			continue
		}
		ids = append(ids, id)
	}
	for _, b := range tx.buffered {
		if b.Type == typ && b.CoalesceKey == key {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

// Commit 原子提交：单次锁内应用全部缓冲行与取代意图（要么全部可见、
// 要么全部不可见），并按首写为准重查交错提交的行。重复 Commit / 已回滚
// 的事务返回错误。
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
	// 应用取代意图：仅命中现存且未被取代的行（候选行被并发取代时后到
	// 让先到，与 READ COMMITTED 重评估口径一致）；候选行未随事务插入或
	// 已被清理则跳过。
	for _, s := range tx.supersede {
		for _, id := range s.ids {
			if ev, ok := d.events[id]; ok && ev.supersededAt.IsZero() {
				ev.supersededAt = s.at
			}
		}
	}
	tx.buffered = nil
	tx.supersede = nil
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
