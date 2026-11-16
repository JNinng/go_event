// Package pgxc 是 go_event 的 Postgres 后端，基于 pgx v5 连接池
// （*pgxpool.Pool）。event / event_claim 两表、FOR UPDATE SKIP LOCKED
// 认领、惰性清理；与 postgrec 共享同一套能力与选项语义（docs/api.md §3）。
//
// 持久结构按 NewDriver 的迁移模式处理：缺省 Create 在首次使用时自动执行
// 基线（持迁移锁）；Verify 只读校验，None 不建也不校验——后两种模式要求
// 表已由部署流程创建。运行账号只授 DML 权限时用 Verify / None；Create
// 需要 DDL 权限。
package pgxc

import (
	"time"

	"github.com/jninng/go_event"
)

// Option 后端构造选项。
type Option func(*config)

type config struct {
	schema    string
	retention time.Duration
	mode      event.MigrateMode
	modeSet   bool
}

// WithSchema 表所在 schema（缺省 public）。
func WithSchema(s string) Option {
	return func(c *config) { c.schema = s }
}

// WithRetention 保留窗口（缺省 7 天）：惰性清理资格为「超窗且无活跃
// 租约，无论终态与否」（design.md §5.2、ADR 0006）。认领消费须将窗口
// 配置为覆盖最坏重试路径：约 重试上限 × (最大退避 + 最大处理时长)，
// 另留心跳间隔与时钟偏差余量。
func WithRetention(d time.Duration) Option {
	return func(c *config) { c.retention = d }
}

// WithMigrateMode 设置运行面迁移模式（缺省 Create）：Create 首次使用时
// 执行基线；Verify 只读校验、不执行 DDL；None 不建表也不校验。该选项
// 只对 NewDriver 有意义，显式传给 Migrate 返回 ErrMigrateModeUnsupported。
func WithMigrateMode(m event.MigrateMode) Option {
	return func(c *config) {
		c.mode = m
		c.modeSet = true
	}
}

func newConfig(opts []Option) *config {
	c := &config{schema: "public", retention: 7 * 24 * time.Hour, mode: event.MigrateCreate}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
