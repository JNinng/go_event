package postgresschema

import (
	"fmt"
	"regexp"

	"github.com/jninng/go_event"
)

// schemaPattern 与各 Postgres 后端一致：schema 名只允许标识符字符，因此
// 可以安全地内插进 DDL。
var schemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateSchema 校验 schema 名；非法名不得进入 DDL 构造。
func ValidateSchema(schema string) error {
	if !schemaPattern.MatchString(schema) {
		return fmt.Errorf("postgresschema: invalid schema name %q", schema)
	}
	return nil
}

// TableEvent / TableEventClaim 是基线表名（未限定）。
const (
	TableEvent      = "event"
	TableEventClaim = "event_claim"
)

// EventColumns 返回 event 表的期望列（顺序即建表顺序）。仅含基线 v1 建表
// 列；基线之后以 ADD COLUMN IF NOT EXISTS 追加的列见 EventVersionColumns
// （独立列对象参与期望对象集合，不进本表定义）。
func EventColumns() []Column {
	return []Column{
		{Name: "id", DataType: "text", NotNull: true},
		{Name: "type", DataType: "text", NotNull: true},
		{Name: "payload", DataType: "bytea", NotNull: true},
		{Name: "metadata", DataType: "jsonb", NotNull: true},
		{Name: "occurred_at", DataType: "timestamp with time zone", NotNull: true},
		{Name: "position", DataType: "bigint", NotNull: true, Sequence: true},
	}
}

// EventVersionColumns 返回 event 表基线之后以 ALTER TABLE ... ADD COLUMN
// IF NOT EXISTS 追加的列（顺序即版本计划 v3 语句顺序）：投递时刻
// deliver_at，可空、无默认值（NULL = 立即可投递，延迟事件 ADR 0013）。
func EventVersionColumns() []Column {
	return []Column{
		{Name: "deliver_at", DataType: "timestamp with time zone"},
	}
}

// EventClaimColumns 返回 event_claim 表的期望列（顺序即建表顺序）。仅含
// 基线 v1 建表列；基线之后以 ADD COLUMN IF NOT EXISTS 追加的列见
// EventClaimVersionColumns（独立列对象参与期望对象集合，不进本表定义）。
func EventClaimColumns() []Column {
	return []Column{
		{Name: "event_id", DataType: "text", NotNull: true},
		{Name: "consumer_type", DataType: "text", NotNull: true},
		{Name: "lease_token", DataType: "text", NotNull: true},
		{Name: "lease_until", DataType: "timestamp with time zone", NotNull: true},
		{Name: "claim_count", DataType: "integer", NotNull: true},
		{Name: "status", DataType: "text", NotNull: true},
		{Name: "next_eligible_at", DataType: "timestamp with time zone", NotNull: true},
	}
}

// EventClaimVersionColumns 返回基线之后以 ALTER TABLE ... ADD COLUMN IF
// NOT EXISTS 追加的 event_claim 列（顺序即版本计划 v2 语句顺序）：错误
// 摘要 last_error 与终态时刻 failed_at，均可空、无默认值。
func EventClaimVersionColumns() []Column {
	return []Column{
		{Name: "last_error", DataType: "text"},
		{Name: "failed_at", DataType: "timestamp with time zone"},
	}
}

// Column 是表列的期望定义。
type Column struct {
	Name     string
	DataType string // 与 Postgres format_type 输出一致，如 "bigint"、"timestamp with time zone"
	NotNull  bool
	// Sequence 表示该列期望带序列默认值（bigserial，pg_attrdef 有 nextval）。
	Sequence bool
}

// ForeignKey 是外键约束的期望引用。
type ForeignKey struct {
	// RefTable 未限定表名（同 schema）；Object.Name 才是限定标识。
	RefTable   string
	RefColumns []string
}

// Object 是期望对象描述。Kind 决定哪些字段有意义：
//   - schema：仅 Name；
//   - table：Columns；
//   - index：Table / KeyColumns / Unique；
//   - constraint：Table / KeyColumns / Unique（唯一）/ ForeignKey（外键）/ 主键。
//
// Name 是限定标识（如 "public.event_pk"），Table 是所属表的未限定名。
type Object struct {
	Kind       event.ObjectKind
	Name       string
	Table      string
	Columns    []Column
	KeyColumns []string
	Unique     bool
	ForeignKey *ForeignKey
}

// Qualified 返回 schema 限定的标识（本包内统一用 schema.table 形式）。
func Qualified(schema, name string) string {
	return schema + "." + name
}

// ExpectedObjects 返回基线与各版本的期望对象集合（schema、两表、约束、
// 索引、版本加列），顺序稳定、与计划语句按序一一配对，供 Verify 与基线
// 接管校验比对。非法 schema 名返回 nil。
func ExpectedObjects(schema string) []Object {
	if ValidateSchema(schema) != nil {
		return nil
	}
	objs := []Object{
		{Kind: event.KindSchema, Name: schema},
		{
			Kind:    event.KindTable,
			Name:    Qualified(schema, TableEvent),
			Table:   TableEvent,
			Columns: EventColumns(),
		},
		{
			Kind:       event.KindConstraint,
			Name:       Qualified(schema, "event_pk"),
			Table:      TableEvent,
			KeyColumns: []string{"id"},
		},
		{
			Kind:       event.KindConstraint,
			Name:       Qualified(schema, "event_type_id_uniq"),
			Table:      TableEvent,
			KeyColumns: []string{"type", "id"},
			Unique:     true,
		},
		{
			Kind:       event.KindIndex,
			Name:       Qualified(schema, "event_type_position_idx"),
			Table:      TableEvent,
			KeyColumns: []string{"type", "position"},
		},
		{
			Kind:    event.KindTable,
			Name:    Qualified(schema, TableEventClaim),
			Table:   TableEventClaim,
			Columns: EventClaimColumns(),
		},
		{
			Kind:       event.KindConstraint,
			Name:       Qualified(schema, "event_claim_pk"),
			Table:      TableEventClaim,
			KeyColumns: []string{"event_id", "consumer_type"},
		},
		{
			Kind:       event.KindConstraint,
			Name:       Qualified(schema, "event_claim_event_id_fkey"),
			Table:      TableEventClaim,
			KeyColumns: []string{"event_id"},
			ForeignKey: &ForeignKey{RefTable: TableEvent, RefColumns: []string{"id"}},
		},
		{
			Kind:       event.KindIndex,
			Name:       Qualified(schema, "event_claim_eligible_idx"),
			Table:      TableEventClaim,
			KeyColumns: []string{"status", "next_eligible_at"},
		},
	}
	// v2 加列版本：独立列对象，与 ALTER TABLE ... ADD COLUMN IF NOT EXISTS
	// 语句按序一一配对；缺失可由迁移补齐。
	for _, col := range EventClaimVersionColumns() {
		objs = append(objs, Object{
			Kind:    event.KindColumn,
			Name:    Qualified(schema, TableEventClaim) + "." + col.Name,
			Table:   TableEventClaim,
			Columns: []Column{col},
		})
	}
	// v3 加列版本：同款独立列对象（event.deliver_at，延迟事件 ADR 0013）。
	for _, col := range EventVersionColumns() {
		objs = append(objs, Object{
			Kind:    event.KindColumn,
			Name:    Qualified(schema, TableEvent) + "." + col.Name,
			Table:   TableEvent,
			Columns: []Column{col},
		})
	}
	return objs
}
