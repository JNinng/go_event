package pgxc

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
)

// wakeChannelPrefix 是跨实例唤醒 NOTIFY 通道前缀，短 schema 通道名 =
// 前缀 + schema（ADR 0013 §2）。Postgres 通道标识上限 63 字节：超限的
// schema 以确定性哈希通道名适配（前缀 + 截断 + 全名哈希，两侧同源派生
// 必然一致），不存在退化路径——任意合法 schema 都有唤醒通道。
const wakeChannelPrefix = "go_event_wake_"

// wakeChannelTrunc 是哈希通道名中保留的 schema 原名前缀长度：前缀 14
// 字节 + 截断 32 字节 + 分隔 1 字节 + FNV-1a 64 位十六进制 16 字节
// = 恰 63 字节（Postgres 标识上限）。schema 只允许标识符字符（ASCII），
// 按字节截断安全。
const wakeChannelTrunc = 32

// appendInsertSQL 是事件 INSERT 的单一语句定义：普通 Append（自管连接，
// 独立提交）与事务性 AppendTx（加入调用方业务事务）共用同一份，避免第
// 二份拷贝漂移（幂等语义一致：同 ID 首写为准，design.md §4）。
//
// 跨实例唤醒信号经 CTE 挂在插入事实之后：仅实际插入行时 NOTIFY（幂等
// 冲突不重发信号）；通道与 payload 只带 schema 名，不带事件内容（ADR 0013
// §2）。pg_notify 随所在事务提交才可见——事务性入队的信号在提交时刻发出，
// 与「提交前发信号只会让消费者空拉一趟」的边界（ADR 0012 §2）天然相容；
// 延迟行的信号同样在插入（事务提交）时发出，不判断 deliver_at——推送只
// 回答「有新行」，不回答「有可投递行」，消费侧空拉一趟由轮询兜底。
const appendInsertSQL = `
WITH ins AS (
	INSERT INTO %s.event (id, type, payload, metadata, occurred_at, deliver_at)
	VALUES ($1, $2, $3, $4::jsonb, $5, $6)
	ON CONFLICT (id) DO NOTHING
	RETURNING 1
)
SELECT pg_notify($7, $8) FROM ins`

// wakeChannel 返回 schema 的唤醒通道名：短 schema（前缀 + 全名不超
// Postgres 标识上限 63 字节）直用全名；超限截取前 wakeChannelTrunc 字节
// 并追加全名的 FNV-1a 64 位十六进制——确定性哈希，LISTEN 与 NOTIFY 两侧
// 经同一函数派生必然一致（ADR 0013 §2）。
func wakeChannel(schema string) string {
	ch := wakeChannelPrefix + schema
	if len(ch) <= 63 {
		return ch
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(schema))
	return wakeChannelPrefix + schema[:wakeChannelTrunc] + "_" + hex.EncodeToString(h.Sum(nil))
}

// deliverAtArg 把信封的 DeliverAt 归一为落库参数：零值存 NULL（立即可
// 投递），非零原样写入 deliver_at。
func deliverAtArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// execAppend 执行事件 INSERT（普通 Append 与事务性 AppendTx 共用同一执行
// 路径）：实际插入行时经 CTE 发跨实例唤醒 NOTIFY（幂等冲突不重发信号；
// 延迟行同样在插入时发，不判断 deliver_at——推送只回答「有新行」，
// ADR 0013 §2）。
func (d *Driver) execAppend(ctx context.Context,
	exec func(context.Context, string, ...any) (pgconn.CommandTag, error),
	e event.RawEnvelope, meta string) error {
	_, err := exec(ctx, fmt.Sprintf(appendInsertSQL, d.schema),
		e.ID, e.Type, e.Payload, meta, e.OccurredAt, deliverAtArg(e.DeliverAt),
		d.wakeChannel, d.schema)
	return err
}

// TransactionalAppender 事务性事件入队（outbox 可选面，不进 Driver 契约，
// ADR 0012）：调用方在自己的业务事务（pgx.Tx）内调用 AppendTx，事件
// INSERT 与业务写共享同一事务，随事务原子提交/回滚——提交与回滚归调用
// 方。形态与 Admin 同款：独立构造路径、与 Driver 共享连接与命名空间选项、
// 不经运行面类型获得（经本构造或组合对象 Store 聚合）；core 不定义 outbox
// 接口（事务载体是后端具体类型）。
//
// 语义边界（ADR 0012；完整论述见 README 能力边界表）：入口 ensure 在调用
// 方事务外的池连接执行、失败上抛；幂等与普通 Append 一致；不触发惰性清
// 理、不发射观测（提交归调用方，提交前发射与落库事实可能矛盾）、不返回
// position（HeadPosition 只锚已提交行）。
type TransactionalAppender struct {
	d *Driver
}

// NewTransactionalAppender 构造事务性入队面：与 NewDriver 共享连接与命名
// 空间选项（同一 schema、同一迁移模式语义——首次操作按模式处理持久结构）。
func NewTransactionalAppender(pool *pgxpool.Pool, opts ...Option) *TransactionalAppender {
	return &TransactionalAppender{d: newDriver(pool, opts...)}
}

// newTransactionalAppenderOn 以给定 Driver 构造事务性入队载体：NewStore
// 复用其唯一 Driver 实例（共享 ensure 状态与失败负缓存），与 newAdminOn
// 同构。
func newTransactionalAppenderOn(d *Driver) *TransactionalAppender {
	return &TransactionalAppender{d: d}
}

// AppendTx 在调用方业务事务 tx 内追加事件（不提交、不回滚——事务控制归
// 调用方）。事件在提交前对外不可见（Fetch / Claim / HeadPosition 均只锚
// 已提交行），提交后经轮询/认领可见——就近唤醒信号不覆盖事务内发布，广
// 播消费下可能被并发提交的更大 position 越过水位而漏投（ADR 0012 §2，需
// 可靠投递用认领消费）。position 在 INSERT 时分配，回滚不回收（Postgres
// 序列不回收，缺口不进入消费契约）。
func (ta *TransactionalAppender) AppendTx(ctx context.Context, tx pgx.Tx, e event.RawEnvelope) error {
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
	if err := ta.d.execAppend(ctx, tx.Exec, e, meta); err != nil {
		return fmt.Errorf("pgxc: append tx: %w", err)
	}
	return nil
}
