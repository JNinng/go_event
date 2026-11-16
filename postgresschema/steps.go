package postgresschema

import (
	"fmt"
	"strings"
	"time"

	"github.com/jninng/go_event"
)

// LockWaitTimeout 是迁移锁的有界等待上限：获取锁超时按瞬时错误处理，
// 不得无限阻塞。
const LockWaitTimeout = 5 * time.Second

// Action 是步骤动作。
type Action string

const (
	// ActionCreate 创建对象（幂等）。
	ActionCreate Action = "create"
	// ActionAlter 补齐既有表上缺失的约束（探测后条件执行）。
	ActionAlter Action = "alter"
)

// Step 是结构化迁移步骤（对象类型 / 标识 / 动作 / 语句 / 是否可事务化 /
// 探测方式 / 是否可验证），不是裸 SQL 字符串。
type Step struct {
	Kind   event.ObjectKind
	Object string // 限定标识，如 "public.event_pk"
	// Table 是该步骤所属表的未限定名（schema 步骤为空）。
	Table  string
	Action Action
	// Statement 是该步骤的 DDL。约束步骤为「探测后条件执行」的 DO 块
	// （Postgres 的 ADD CONSTRAINT 没有 IF NOT EXISTS）。
	Statement string
	// Transactional 表示语句可放入跨语句事务（基线全部可事务化）。
	Transactional bool
	// Probe 是该步骤的探测方式（执行器在 step 路径复用）。
	Probe event.ProbeSupport
	// Verifiable 表示该步骤的期望定义可被 Verify 比对。
	Verifiable bool
}

// Baseline 返回基线的结构化步骤，顺序稳定（被引用的对象先于引用者）。
func Baseline(schema string) ([]Step, error) {
	if err := ValidateSchema(schema); err != nil {
		return nil, err
	}
	steps := []Step{
		{
			Kind:          event.KindSchema,
			Object:        schema,
			Action:        ActionCreate,
			Statement:     fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, schema),
			Transactional: true,
			Probe:         event.ProbeExistence,
			Verifiable:    true,
		},
		{
			Kind:          event.KindTable,
			Object:        Qualified(schema, TableEvent),
			Table:         TableEvent,
			Action:        ActionCreate,
			Statement:     createEventTable(schema),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindConstraint,
			Object:        Qualified(schema, "event_pk"),
			Table:         TableEvent,
			Action:        ActionAlter,
			Statement:     ensurePrimaryKey(schema, TableEvent, "event_pk", []string{"id"}),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindConstraint,
			Object:        Qualified(schema, "event_type_id_uniq"),
			Table:         TableEvent,
			Action:        ActionAlter,
			Statement:     ensureUnique(schema, TableEvent, "event_type_id_uniq", []string{"type", "id"}),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindIndex,
			Object:        Qualified(schema, "event_type_position_idx"),
			Table:         TableEvent,
			Action:        ActionCreate,
			Statement:     fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_type_position_idx ON %s.%s (type, position);`, schema, TableEvent),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindTable,
			Object:        Qualified(schema, TableEventClaim),
			Table:         TableEventClaim,
			Action:        ActionCreate,
			Statement:     createEventClaimTable(schema),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindConstraint,
			Object:        Qualified(schema, "event_claim_pk"),
			Table:         TableEventClaim,
			Action:        ActionAlter,
			Statement:     ensurePrimaryKey(schema, TableEventClaim, "event_claim_pk", []string{"event_id", "consumer_type"}),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindConstraint,
			Object:        Qualified(schema, "event_claim_event_id_fkey"),
			Table:         TableEventClaim,
			Action:        ActionAlter,
			Statement:     ensureForeignKey(schema, TableEventClaim, "event_claim_event_id_fkey", []string{"event_id"}, TableEvent, []string{"id"}),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
		{
			Kind:          event.KindIndex,
			Object:        Qualified(schema, "event_claim_eligible_idx"),
			Table:         TableEventClaim,
			Action:        ActionCreate,
			Statement:     fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_claim_eligible_idx ON %s.%s (status, next_eligible_at);`, schema, TableEventClaim),
			Transactional: true,
			Probe:         event.ProbeDefinition,
			Verifiable:    true,
		},
	}
	return steps, nil
}

// Statements 返回基线语句（不含迁移锁），按执行顺序排列。
func Statements(schema string) ([]string, error) {
	steps, err := Baseline(schema)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, step.Statement)
	}
	return out, nil
}

// SQL 返回手工可执行的迁移脚本：以 BEGIN 开始，随后设 lock_timeout 并取
// 迁移锁（锁语句位于全部基线 DDL 之前），末尾 COMMIT。锁键含 schema 命名
// 空间；锁等待由 lock_timeout 限定，不会无限阻塞。
func SQL(schema string) (string, error) {
	stmts, err := Statements(schema)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString("BEGIN;\n\n")
	fmt.Fprintf(&sb, "SET LOCAL lock_timeout = '%s';\n\n", lockTimeoutLiteral())
	fmt.Fprintf(&sb, "SELECT pg_advisory_xact_lock(hashtext('%s'));\n\n", LockKey(schema))
	for _, stmt := range stmts {
		sb.WriteString(stmt)
		sb.WriteString("\n\n")
	}
	sb.WriteString("COMMIT;\n")
	return sb.String(), nil
}

// LockKey 返回迁移锁键：含 schema 命名空间，不同 schema 不互相阻塞。
func LockKey(schema string) string {
	return "go_event:" + schema
}

// TryLockStatement 是事务级 try-lock 语句（参数为 LockKey 的返回值）。
// 执行器用 try-lock + 退避实现有界等待。
const TryLockStatement = `SELECT pg_try_advisory_xact_lock(hashtext($1))`

func lockTimeoutLiteral() string {
	return fmt.Sprintf("%dms", LockWaitTimeout.Milliseconds())
}

func createEventTable(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	id          text        NOT NULL,
	type        text        NOT NULL,
	payload     bytea       NOT NULL,
	metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
	occurred_at timestamptz NOT NULL,
	position    bigserial   NOT NULL,
	CONSTRAINT event_pk PRIMARY KEY (id),
	CONSTRAINT event_type_id_uniq UNIQUE (type, id)
);`, schema, TableEvent)
}

func createEventClaimTable(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	event_id         text        NOT NULL REFERENCES %[1]s.%[3]s (id) ON DELETE CASCADE,
	consumer_type    text        NOT NULL,
	lease_token      text        NOT NULL,
	lease_until      timestamptz NOT NULL,
	claim_count      integer     NOT NULL DEFAULT 0,
	status           text        NOT NULL,
	next_eligible_at timestamptz NOT NULL DEFAULT '-infinity'::timestamptz,
	CONSTRAINT event_claim_pk PRIMARY KEY (event_id, consumer_type)
);`, schema, TableEventClaim, TableEvent)
}

// ensurePrimaryKey 探测表上是否已有主键（任意名称），缺失则补建。
func ensurePrimaryKey(schema, table, name string, columns []string) string {
	return fmt.Sprintf(`DO $go_event$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_constraint
		WHERE conrelid = '%[1]s.%[2]s'::regclass AND contype = 'p'
	) THEN
		ALTER TABLE %[1]s.%[2]s ADD CONSTRAINT %[3]s PRIMARY KEY (%[4]s);
	END IF;
END
$go_event$;`, schema, table, name, strings.Join(columns, ", "))
}

// ensureUnique 探测表上是否已有同列的 UNIQUE 约束（ADD CONSTRAINT 无
// IF NOT EXISTS，必须探测后条件执行）。
func ensureUnique(schema, table, name string, columns []string) string {
	return fmt.Sprintf(`DO $go_event$
BEGIN
	IF NOT EXISTS (
		SELECT 1
		FROM pg_constraint c
		WHERE c.conrelid = '%[1]s.%[2]s'::regclass
		  AND c.contype = 'u'
		  AND (
			SELECT array_agg(a.attname::text ORDER BY k.ord)
			FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord)
			JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
		  ) = ARRAY[%[3]s]::text[]
	) THEN
		ALTER TABLE %[1]s.%[2]s ADD CONSTRAINT %[4]s UNIQUE (%[5]s);
	END IF;
END
$go_event$;`, schema, table, quoteList(columns), name, strings.Join(columns, ", "))
}

// ensureForeignKey 探测表上是否已有同列同引用的外键，缺失则补建。
func ensureForeignKey(schema, table, name string, columns []string, refTable string, refColumns []string) string {
	return fmt.Sprintf(`DO $go_event$
BEGIN
	IF NOT EXISTS (
		SELECT 1
		FROM pg_constraint c
		WHERE c.conrelid = '%[1]s.%[2]s'::regclass
		  AND c.contype = 'f'
		  AND c.confrelid = '%[1]s.%[3]s'::regclass
		  AND (
			SELECT array_agg(a.attname::text ORDER BY k.ord)
			FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord)
			JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
		  ) = ARRAY[%[4]s]::text[]
	) THEN
		ALTER TABLE %[1]s.%[2]s ADD CONSTRAINT %[5]s
			FOREIGN KEY (%[6]s) REFERENCES %[1]s.%[3]s (%[7]s) ON DELETE CASCADE;
	END IF;
END
$go_event$;`, schema, table, refTable, quoteList(columns), name,
		strings.Join(columns, ", "), strings.Join(refColumns, ", "))
}

func quoteList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = "'" + item + "'"
	}
	return strings.Join(quoted, ", ")
}
