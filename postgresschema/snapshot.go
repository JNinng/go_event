package postgresschema

import (
	"context"
	"fmt"
	"strings"

	"github.com/jninng/go_event"
)

// Rows 是只读查询结果的驱动无关视图（*sql.Rows 直接满足；pgx.Rows 由
// 后端适配）。本包不 import 任何驱动。
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Queryer 是只读查询能力（只读事务或连接池均可适配）。
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

const (
	schemaExistsQuery = `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`

	tablesQuery = `
SELECT c.relname, c.relkind::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = ANY (ARRAY['event', 'event_claim'])
ORDER BY c.relname`

	columnsQuery = `
SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
       COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1 AND c.relname = ANY (ARRAY['event', 'event_claim'])
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum`

	constraintsQuery = `
SELECT c.conname, c.contype::text, t.relname,
       COALESCE(array_to_string(ARRAY(
           SELECT a.attname
           FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord)
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
           ORDER BY k.ord), ','), ''),
       COALESCE(rt.relname, ''),
       COALESCE(array_to_string(ARRAY(
           SELECT a.attname
           FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord)
           JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum
           ORDER BY k.ord), ','), '')
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_class rt ON rt.oid = c.confrelid
WHERE n.nspname = $1 AND t.relname = ANY (ARRAY['event', 'event_claim'])
  AND c.contype IN ('p', 'u', 'f')
ORDER BY t.relname, c.conname`

	indexesQuery = `
SELECT ic.relname, tc.relname, i.indisunique,
       COALESCE(array_to_string(ARRAY(
           SELECT a.attname
           FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
           JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
           WHERE k.attnum > 0
           ORDER BY k.ord), ','), '')
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace
WHERE n.nspname = $1
  AND (ic.relname = ANY (ARRAY['event_type_position_idx', 'event_claim_eligible_idx'])
       OR tc.relname = ANY (ARRAY['event', 'event_claim']))
ORDER BY ic.relname`
)

// Snapshot 是从系统目录读到的当前对象状态（仅覆盖基线对象所在 schema）。
type Snapshot struct {
	SchemaExists bool
	Tables       map[string]TableState
	Constraints  map[string]ConstraintState
	Indexes      map[string]IndexState
}

// TableState 是已存在表的目录快照。
type TableState struct {
	RelKind string // pg_class.relkind：r/p 为可承载数据的表
	Columns []ColumnState
}

// ColumnState 是已存在列的快照。
type ColumnState struct {
	Name     string
	DataType string
	NotNull  bool
	// HasSequenceDefault 表示该列默认值含 nextval（bigserial/sequence）。
	HasSequenceDefault bool
}

// ConstraintState 是主键/唯一/外键约束的快照（表名与引用表名未限定）。
type ConstraintState struct {
	Name       string
	Type       string // p / u / f
	Table      string
	Columns    []string
	RefTable   string
	RefColumns []string
}

// IndexState 是索引的快照（表名未限定）。
type IndexState struct {
	Name    string
	Table   string
	Unique  bool
	Columns []string
}

// ReadSnapshot 在给定查询器上读取系统目录，构造基线相关对象的快照。
// 调用方应使用只读事务保证多次查询看到一致状态。
func ReadSnapshot(ctx context.Context, q Queryer, schema string) (Snapshot, error) {
	snap := Snapshot{
		Tables:      map[string]TableState{},
		Constraints: map[string]ConstraintState{},
		Indexes:     map[string]IndexState{},
	}
	if err := readSchemaExists(ctx, q, schema, &snap); err != nil {
		return Snapshot{}, err
	}
	if err := readTables(ctx, q, schema, &snap); err != nil {
		return Snapshot{}, err
	}
	if err := readColumns(ctx, q, schema, &snap); err != nil {
		return Snapshot{}, err
	}
	if err := readConstraints(ctx, q, schema, &snap); err != nil {
		return Snapshot{}, err
	}
	if err := readIndexes(ctx, q, schema, &snap); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

func readSchemaExists(ctx context.Context, q Queryer, schema string, snap *Snapshot) error {
	rows, err := q.QueryContext(ctx, schemaExistsQuery, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: read schema existence: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("postgresschema: read schema existence: %w", err)
		}
		return fmt.Errorf("postgresschema: read schema existence: no row")
	}
	if err := rows.Scan(&snap.SchemaExists); err != nil {
		return fmt.Errorf("postgresschema: read schema existence: %w", err)
	}
	return nil
}

func readTables(ctx context.Context, q Queryer, schema string, snap *Snapshot) error {
	rows, err := q.QueryContext(ctx, tablesQuery, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: read tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return fmt.Errorf("postgresschema: read tables: %w", err)
		}
		state := snap.Tables[name]
		state.RelKind = kind
		snap.Tables[name] = state
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgresschema: read tables: %w", err)
	}
	return nil
}

func readColumns(ctx context.Context, q Queryer, schema string, snap *Snapshot) error {
	rows, err := q.QueryContext(ctx, columnsQuery, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: read columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		var col ColumnState
		var defaultExpr string
		if err := rows.Scan(&table, &col.Name, &col.DataType, &col.NotNull, &defaultExpr); err != nil {
			return fmt.Errorf("postgresschema: read columns: %w", err)
		}
		col.HasSequenceDefault = strings.Contains(defaultExpr, "nextval(")
		state := snap.Tables[table]
		state.Columns = append(state.Columns, col)
		snap.Tables[table] = state
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgresschema: read columns: %w", err)
	}
	return nil
}

func readConstraints(ctx context.Context, q Queryer, schema string, snap *Snapshot) error {
	rows, err := q.QueryContext(ctx, constraintsQuery, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: read constraints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c ConstraintState
		var columns, refColumns string
		if err := rows.Scan(&c.Name, &c.Type, &c.Table, &columns, &c.RefTable, &refColumns); err != nil {
			return fmt.Errorf("postgresschema: read constraints: %w", err)
		}
		c.Columns = splitColumns(columns)
		c.RefColumns = splitColumns(refColumns)
		snap.Constraints[c.Name] = c
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgresschema: read constraints: %w", err)
	}
	return nil
}

func readIndexes(ctx context.Context, q Queryer, schema string, snap *Snapshot) error {
	rows, err := q.QueryContext(ctx, indexesQuery, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: read indexes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var idx IndexState
		var columns string
		if err := rows.Scan(&name, &idx.Table, &idx.Unique, &columns); err != nil {
			return fmt.Errorf("postgresschema: read indexes: %w", err)
		}
		idx.Name = name
		idx.Columns = splitColumns(columns)
		snap.Indexes[name] = idx
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgresschema: read indexes: %w", err)
	}
	return nil
}

// Diff 比对期望对象与快照，按期望顺序返回稳定排序的问题列表。
// 只报告缺失与定义不一致；多余对象（如更高版本新增的列）不影响判定。
func Diff(expected []Object, snap Snapshot) event.VerifyReport {
	var issues []event.VerifyIssue
	for _, obj := range expected {
		switch obj.Kind {
		case event.KindSchema:
			if !snap.SchemaExists {
				issues = append(issues, newIssue(obj.Name, obj.Kind, event.ProblemMissing,
					"run the migration (Create) or the SQL script to create the schema"))
			}
		case event.KindTable:
			issues = append(issues, diffTable(obj, snap)...)
		case event.KindConstraint:
			issues = append(issues, diffConstraint(obj, snap)...)
		case event.KindIndex:
			issues = append(issues, diffIndex(obj, snap)...)
		}
	}
	return event.VerifyReport{Issues: issues}
}

func diffTable(obj Object, snap Snapshot) []event.VerifyIssue {
	state, ok := snap.Tables[obj.Table]
	if !ok {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemMissing,
			"run the migration (Create) or the SQL script to create the table")}
	}
	if state.RelKind != "r" && state.RelKind != "p" {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemOutdated,
			"replace the conflicting relation with a table of the planned definition")}
	}
	var issues []event.VerifyIssue
	for _, want := range obj.Columns {
		actual, found := findColumn(state, want.Name)
		name := obj.Name + "." + want.Name
		if !found {
			issues = append(issues, newIssue(name, event.KindColumn, event.ProblemMissing,
				"add the column manually to match the plan (the migration does not alter existing tables)"))
			continue
		}
		if actual.DataType != want.DataType || actual.NotNull != want.NotNull ||
			(want.Sequence && !actual.HasSequenceDefault) {
			issues = append(issues, newIssue(name, event.KindColumn, event.ProblemOutdated,
				"align the column definition with the plan manually (not auto-repaired)"))
		}
	}
	return issues
}

func diffConstraint(obj Object, snap Snapshot) []event.VerifyIssue {
	if obj.ForeignKey != nil {
		return diffForeignKey(obj, snap)
	}
	if obj.Unique {
		return diffUnique(obj, snap)
	}
	pk, ok := findConstraint(snap, obj.Table, func(c ConstraintState) bool { return c.Type == "p" })
	if !ok {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemMissing,
			"run the migration (Create) or the SQL script to add the primary key")}
	}
	if !equalColumns(pk.Columns, obj.KeyColumns) {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemOutdated,
			"align the primary key columns with the plan manually (not auto-repaired)")}
	}
	return nil
}

func diffUnique(obj Object, snap Snapshot) []event.VerifyIssue {
	if actual, ok := snap.Constraints[unqualified(obj.Name)]; ok && actual.Table == obj.Table {
		if !equalColumns(actual.Columns, obj.KeyColumns) {
			return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemOutdated,
				"align the unique constraint columns with the plan manually (not auto-repaired)")}
		}
		return nil
	}
	_, ok := findConstraint(snap, obj.Table, func(c ConstraintState) bool {
		return c.Type == "u" && equalColumns(c.Columns, obj.KeyColumns)
	})
	if ok {
		return nil // 同列的唯一约束已存在（名称不同不影响语义）
	}
	return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemMissing,
		"run the migration (Create) or the SQL script to add the unique constraint")}
}

func diffForeignKey(obj Object, snap Snapshot) []event.VerifyIssue {
	if actual, ok := snap.Constraints[unqualified(obj.Name)]; ok && actual.Table == obj.Table {
		if !equalColumns(actual.Columns, obj.KeyColumns) ||
			actual.RefTable != obj.ForeignKey.RefTable ||
			!equalColumns(actual.RefColumns, obj.ForeignKey.RefColumns) {
			return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemOutdated,
				"align the foreign key definition with the plan manually (not auto-repaired)")}
		}
		return nil
	}
	_, ok := findConstraint(snap, obj.Table, func(c ConstraintState) bool {
		return c.Type == "f" && equalColumns(c.Columns, obj.KeyColumns) &&
			c.RefTable == obj.ForeignKey.RefTable &&
			equalColumns(c.RefColumns, obj.ForeignKey.RefColumns)
	})
	if ok {
		return nil
	}
	return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemMissing,
		"run the migration (Create) or the SQL script to add the foreign key")}
}

func diffIndex(obj Object, snap Snapshot) []event.VerifyIssue {
	idx, ok := snap.Indexes[unqualified(obj.Name)]
	if !ok {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemMissing,
			"run the migration (Create) or the SQL script to create the index")}
	}
	if idx.Table != obj.Table || idx.Unique != obj.Unique || !equalColumns(idx.Columns, obj.KeyColumns) {
		return []event.VerifyIssue{newIssue(obj.Name, obj.Kind, event.ProblemOutdated,
			"align the index definition with the plan manually (not auto-repaired)")}
	}
	return nil
}

// Repairable 报告问题集合是否能由版本计划补齐：缺失的 schema/表/索引/
// 约束可由计划的幂等语句构造；缺失的基线列不能（其构造语句是
// CREATE TABLE IF NOT EXISTS，对已存在的表不生效，属定义偏差，必须人工
// 处置；后续版本以 ADD COLUMN IF NOT EXISTS 加列时再放开该档）；定义
// 冲突（outdated）不能，必须人工处置。
func Repairable(report event.VerifyReport) bool {
	for _, issue := range report.Issues {
		if issue.Problem != event.ProblemMissing {
			return false
		}
		switch issue.Kind {
		case event.KindSchema, event.KindTable, event.KindIndex, event.KindConstraint:
		default:
			return false
		}
	}
	return true
}

func newIssue(object string, kind event.ObjectKind, problem event.VerifyProblem, action string) event.VerifyIssue {
	return event.VerifyIssue{Object: object, Kind: kind, Problem: problem, Action: action}
}

func findColumn(state TableState, name string) (ColumnState, bool) {
	for _, col := range state.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnState{}, false
}

func findConstraint(snap Snapshot, table string, match func(ConstraintState) bool) (ConstraintState, bool) {
	for _, c := range snap.Constraints {
		if c.Table == table && match(c) {
			return c, true
		}
	}
	return ConstraintState{}, false
}

func unqualified(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

func splitColumns(joined string) []string {
	if joined == "" {
		return nil
	}
	return strings.Split(joined, ",")
}

func equalColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
