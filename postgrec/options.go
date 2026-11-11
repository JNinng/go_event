// Package postgrec 是 go_event 的 Postgres 后端，基于标准库 database/sql。
// event / event_claim 两表、幂等 DDL、FOR UPDATE SKIP LOCKED 认领、
// 惰性清理（design.md §7、§9.1）。
//
// 连接由使用方建立并注册驱动（如 pgx/v5/stdlib），本包不携带任何
// 数据库驱动。
package postgrec

import "time"

// Option 后端构造选项。
type Option func(*config)

type config struct {
	schema    string
	retention time.Duration
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

func newConfig(opts []Option) *config {
	c := &config{schema: "public", retention: 7 * 24 * time.Hour}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
