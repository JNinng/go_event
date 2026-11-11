package pgxc

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Migrate 幂等 DDL：创建 event / event_claim 两表与唯一约束
// （event(id)、event(type,id)、event_claim(event_id,consumer_type)），
// 与 postgrec.Migrate 相同的两表与唯一约束；可重复执行、不破坏既有
// 数据与状态（design.md §8）。NewDriver 也会首次使用时自动执行本迁移。
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error {
	cfg := newConfig(opts)
	if !schemaPattern.MatchString(cfg.schema) {
		return fmt.Errorf("pgxc: invalid schema name %q", cfg.schema)
	}
	ddl := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, cfg.schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.event (
	id          text        NOT NULL,
	type        text        NOT NULL,
	payload     bytea       NOT NULL,
	metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
	occurred_at timestamptz NOT NULL,
	position    bigserial   NOT NULL,
	CONSTRAINT event_pk PRIMARY KEY (id),
	CONSTRAINT event_type_id_uniq UNIQUE (type, id)
)`, cfg.schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_type_position_idx ON %[1]s.event (type, position)`, cfg.schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.event_claim (
	event_id         text        NOT NULL REFERENCES %[1]s.event (id) ON DELETE CASCADE,
	consumer_type    text        NOT NULL,
	lease_token      text        NOT NULL,
	lease_until      timestamptz NOT NULL,
	claim_count      integer     NOT NULL DEFAULT 0,
	status           text        NOT NULL,
	next_eligible_at timestamptz NOT NULL DEFAULT '-infinity'::timestamptz,
	CONSTRAINT event_claim_pk PRIMARY KEY (event_id, consumer_type)
)`, cfg.schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_claim_eligible_idx ON %[1]s.event_claim (status, next_eligible_at)`, cfg.schema),
	}
	for _, stmt := range ddl {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgxc: migrate: %w", err)
		}
	}
	return nil
}
