package postgresschema

import (
	"fmt"
	"strings"
	"time"
)

// LockWaitTimeout 是迁移锁的有界等待上限：获取锁超时按瞬时错误处理，
// 不得无限阻塞。
const LockWaitTimeout = 5 * time.Second

// TryLockStatement 是事务级 try-lock 语句（参数为 LockKey 的返回值）。
// 执行器用 try-lock + 退避实现有界等待。
const TryLockStatement = `SELECT pg_try_advisory_xact_lock(hashtext($1))`

// LockKey 返回迁移锁键：含 schema 命名空间，不同 schema 不互相阻塞。
func LockKey(schema string) string {
	return "go_event:" + schema
}

// Version 是版本计划中的一个版本：版本号 + 幂等语句列表。语句全部幂等
// （expand-only：建表/加列/建索引），一条语句恰好构造期望对象集合中的
// 一个对象。版本号自基线 1 起严格递增、每版至少一条语句；版本号只存在
// 于计划中，供评审与编排，不在库内落记录（对象集合即事实）。
type Version struct {
	Number     int
	Statements []string
}

// PlannedStatement 是版本计划中的一条语句及其构造的期望对象（计划语句与
// ExpectedObjects 按序一一对应）；共享执行器的错误分类与测试钩子按它
// 定位对象。
type PlannedStatement struct {
	SQL    string
	Object Object
}

// Plan 返回完整版本计划：基线 v1 与后续版本，按版本号递增排列。后续
// 版本以追加方式进入（建表/加列/建索引），不修改既有版本语句。
func Plan(schema string) ([]Version, error) {
	stmts, err := baselineStatements(schema)
	if err != nil {
		return nil, err
	}
	versions := []Version{{Number: 1, Statements: stmts}}
	v2, err := v2Statements(schema)
	if err != nil {
		return nil, err
	}
	versions = append(versions, Version{Number: 2, Statements: v2})
	v3, err := v3Statements(schema)
	if err != nil {
		return nil, err
	}
	versions = append(versions, Version{Number: 3, Statements: v3})
	v4, err := v4Statements(schema)
	if err != nil {
		return nil, err
	}
	versions = append(versions, Version{Number: 4, Statements: v4})
	// 后续版本在此按 Number 递增追加；每版至少一条语句。
	return versions, nil
}

// PlanStatements 返回计划内全部语句（不含迁移锁），按执行顺序排列。
func PlanStatements(schema string) ([]string, error) {
	plan, err := Plan(schema)
	if err != nil {
		return nil, err
	}
	return flattenPlan(plan), nil
}

// PlannedStatements 返回计划内全部语句，与 ExpectedObjects 按序一一对应
// （第 i 条语句恰好构造第 i 个期望对象）。两者长度不一致属计划缺陷，
// 返回错误而不是静默错位。
func PlannedStatements(schema string) ([]PlannedStatement, error) {
	stmts, err := PlanStatements(schema)
	if err != nil {
		return nil, err
	}
	objects := ExpectedObjects(schema)
	if objects == nil {
		return nil, fmt.Errorf("postgresschema: plan: invalid schema %q", schema)
	}
	if len(stmts) != len(objects) {
		return nil, fmt.Errorf(
			"postgresschema: plan defect: %d statements vs %d expected objects (must pair 1:1)",
			len(stmts), len(objects))
	}
	paired := make([]PlannedStatement, len(stmts))
	for i := range stmts {
		paired[i] = PlannedStatement{SQL: stmts[i], Object: objects[i]}
	}
	return paired, nil
}

// Statements 返回计划内全部语句（不含迁移锁），按执行顺序排列。与执行器
// 共用同一事实源（Plan），不维护第二份语句定义。
func Statements(schema string) ([]string, error) {
	return PlanStatements(schema)
}

// SQL 返回手工可执行的迁移脚本：以 BEGIN 开始，随后设 lock_timeout 并取
// 迁移锁（锁语句位于全部 DDL 之前），随后依次输出计划内全部语句
// （基线 + 各版本，全部幂等），末尾 COMMIT。脚本由版本计划派生、整段可
// 重放（中断重跑收敛）；人工执行必须整段执行，不得拆分跳过。
func SQL(schema string) (string, error) {
	stmts, err := PlanStatements(schema)
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

func flattenPlan(plan []Version) []string {
	total := 0
	for _, v := range plan {
		total += len(v.Statements)
	}
	out := make([]string, 0, total)
	for _, v := range plan {
		out = append(out, v.Statements...)
	}
	return out
}

func lockTimeoutLiteral() string {
	return fmt.Sprintf("%dms", LockWaitTimeout.Milliseconds())
}

// baselineStatements 返回基线 v1 语句，顺序与 ExpectedObjects 一致（被
// 引用的对象先于引用者）。建表语句不内联约束：约束统一走探测后条件执行
// 的 DO 块（ADD CONSTRAINT 无 IF NOT EXISTS，探测使语句可重放）。
func baselineStatements(schema string) ([]string, error) {
	if err := ValidateSchema(schema); err != nil {
		return nil, err
	}
	return []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, schema),
		createEventTable(schema),
		ensurePrimaryKey(schema, TableEvent, "event_pk", []string{"id"}),
		ensureUnique(schema, TableEvent, "event_type_id_uniq", []string{"type", "id"}),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_type_position_idx ON %s.%s (type, position);`, schema, TableEvent),
		createEventClaimTable(schema),
		ensurePrimaryKey(schema, TableEventClaim, "event_claim_pk", []string{"event_id", "consumer_type"}),
		ensureForeignKey(schema, TableEventClaim, "event_claim_event_id_fkey", []string{"event_id"}, TableEvent, []string{"id"}),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_claim_eligible_idx ON %s.%s (status, next_eligible_at);`, schema, TableEventClaim),
	}, nil
}

func createEventTable(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	id          text        NOT NULL,
	type        text        NOT NULL,
	payload     bytea       NOT NULL,
	metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
	occurred_at timestamptz NOT NULL,
	position    bigserial   NOT NULL
);`, schema, TableEvent)
}

func createEventClaimTable(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	event_id         text        NOT NULL,
	consumer_type    text        NOT NULL,
	lease_token      text        NOT NULL,
	lease_until      timestamptz NOT NULL,
	claim_count      integer     NOT NULL DEFAULT 0,
	status           text        NOT NULL,
	next_eligible_at timestamptz NOT NULL DEFAULT '-infinity'::timestamptz
);`, schema, TableEventClaim)
}

// v2Statements 加列版本：event_claim 追加错误摘要列（last_error）与终态
// 时刻列（failed_at），支撑失败摘要落库（docs/design.md §5.3）。语句幂等
// 且 expand-only，一条语句恰好构造期望对象集合中的一个列对象；列均可空、
// 无默认值，对既有行与旧二进制无害。
func v2Statements(schema string) ([]string, error) {
	if err := ValidateSchema(schema); err != nil {
		return nil, err
	}
	cols := EventClaimVersionColumns()
	stmts := make([]string, 0, len(cols))
	for _, col := range cols {
		stmts = append(stmts, fmt.Sprintf(
			`ALTER TABLE %[1]s.%[2]s ADD COLUMN IF NOT EXISTS %[3]s %[4]s;`,
			schema, TableEventClaim, col.Name, ddlType(col.DataType)))
	}
	return stmts, nil
}

// v3Statements 加列版本：event 追加投递时刻列（deliver_at），支撑延迟
// 事件定时投递（docs/design.md §9.2、ADR 0013）。沿 v2 先例：语句幂等
// 且 expand-only，一条语句恰好构造期望对象集合中的一个列对象；列可空、
// 无默认值（NULL = 立即可投递），对既有行与旧二进制无害。
func v3Statements(schema string) ([]string, error) {
	if err := ValidateSchema(schema); err != nil {
		return nil, err
	}
	cols := EventVersionColumns()
	stmts := make([]string, 0, len(cols))
	for _, col := range cols {
		stmts = append(stmts, fmt.Sprintf(
			`ALTER TABLE %[1]s.%[2]s ADD COLUMN IF NOT EXISTS %[3]s %[4]s;`,
			schema, TableEvent, col.Name, ddlType(col.DataType)))
	}
	return stmts, nil
}

// v4Statements 合并投递版本：event 追加合并键列（coalesce_key）与取代
// 时刻列（superseded_at），并建同键取代查找索引（docs/design.md §9.2、
// ADR 0014）。沿 v2/v3 先例：语句幂等且 expand-only，一条语句恰好构造
// 期望对象集合中的一个对象；列可空、无默认值（NULL = 未设置 / 未取代），
// 对既有行与旧二进制无害——合并是优化不是正确性保证。索引为部分索引
// （仅覆盖参与合并的行），快照比对只看名称/表/键列，谓词不参与。
func v4Statements(schema string) ([]string, error) {
	if err := ValidateSchema(schema); err != nil {
		return nil, err
	}
	cols := EventCoalesceVersionColumns()
	stmts := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		stmts = append(stmts, fmt.Sprintf(
			`ALTER TABLE %[1]s.%[2]s ADD COLUMN IF NOT EXISTS %[3]s %[4]s;`,
			schema, TableEvent, col.Name, ddlType(col.DataType)))
	}
	stmts = append(stmts, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s.%[2]s (type, coalesce_key) WHERE coalesce_key IS NOT NULL;`,
		schema, TableEvent, EventCoalesceIndex))
	return stmts, nil
}

// ddlType 把列的期望类型（format_type 口径，见 Column.DataType）映射为
// 建列 DDL 类型字面量。
func ddlType(dataType string) string {
	if dataType == "timestamp with time zone" {
		return "timestamptz"
	}
	return dataType
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

// ensureForeignKey 探测表上是否已有同键列、同引用表同引用列的外键，缺失
// 则补建。ON DELETE 动作不参与探测，与快照比对口径一致。
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
		  AND (
			SELECT array_agg(a.attname::text ORDER BY k.ord)
			FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord)
			JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum
		  ) = ARRAY[%[8]s]::text[]
	) THEN
		ALTER TABLE %[1]s.%[2]s ADD CONSTRAINT %[5]s
			FOREIGN KEY (%[6]s) REFERENCES %[1]s.%[3]s (%[7]s) ON DELETE CASCADE;
	END IF;
END
$go_event$;`, schema, table, refTable, quoteList(columns), name,
		strings.Join(columns, ", "), strings.Join(refColumns, ", "), quoteList(refColumns))
}

func quoteList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = "'" + item + "'"
	}
	return strings.Join(quoted, ", ")
}
