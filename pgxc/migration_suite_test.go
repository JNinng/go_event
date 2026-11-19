package pgxc_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/pgxc"
	"github.com/jninng/go_event/postgresschema"
)

// TestMigrationSuite 把 pgxc 接入迁移契约测试载体。TEST_POSTGRES_DSN 未设置
// 时整体跳过；schema 名带随机后缀，并发执行亦不撞名。
func TestMigrationSuite(t *testing.T) {
	eventtest.RunMigrationSuite(t, &suiteBackend{pool: migrationPool(t)})
}

// suiteBackend 是 pgxc 的 MigrationBackend 适配器：每个子测试使用独立
// schema；支持 before/after statement 故障注入（映射到执行器语句钩子）。
type suiteBackend struct {
	pool        *pgxpool.Pool
	schema      string
	restoreHook func()
}

func (b *suiteBackend) NewDriver(t *testing.T, mode event.MigrateMode) event.Driver {
	t.Helper()
	if b.schema == "" {
		t.Fatal("suite backend has no schema; Reset must precede NewDriver")
	}
	return pgxc.NewDriver(b.pool, pgxc.WithSchema(b.schema), pgxc.WithMigrateMode(mode))
}

func (b *suiteBackend) Migrate(ctx context.Context) error {
	return pgxc.Migrate(ctx, b.pool, pgxc.WithSchema(b.schema))
}

func (b *suiteBackend) Verify(ctx context.Context) (event.VerifyReport, error) {
	return pgxc.Verify(ctx, b.pool, pgxc.WithSchema(b.schema))
}

func (b *suiteBackend) Statements() ([]string, error) {
	return pgxc.Statements(pgxc.WithSchema(b.schema))
}

func (b *suiteBackend) SQL() (string, error) {
	return pgxc.SQL(pgxc.WithSchema(b.schema))
}

func (b *suiteBackend) StatementsForSchema(schema string) ([]string, error) {
	return pgxc.Statements(pgxc.WithSchema(schema))
}

func (b *suiteBackend) SQLForSchema(schema string) (string, error) {
	return pgxc.SQL(pgxc.WithSchema(schema))
}

func (b *suiteBackend) Schema() string {
	return b.schema
}

func (b *suiteBackend) ExpectedObjects() []string {
	objs := postgresschema.ExpectedObjects(b.schema)
	names := make([]string, 0, len(objs))
	for _, obj := range objs {
		names = append(names, obj.Name)
	}
	return names
}

// RelationExists 用 to_regclass 直接判定限定标识对应的关系是否存在。
func (b *suiteBackend) RelationExists(object string) bool {
	var reg *string
	if err := b.pool.QueryRow(context.Background(),
		"SELECT to_regclass($1)::text", object).Scan(&reg); err != nil {
		return false
	}
	return reg != nil && *reg != ""
}

func (b *suiteBackend) Capabilities() event.MigrationCapabilities {
	return pgxc.Capabilities()
}

func (b *suiteBackend) Prepare(t *testing.T, scene eventtest.MigrationScene) {
	t.Helper()
	ctx := t.Context()
	switch scene {
	case eventtest.MigrationSceneEmpty:
		// 空库：Reset 已生成未创建的 schema 名。
	case eventtest.MigrationSceneComplete:
		b.execBaseline(t, ctx, "")
	case eventtest.MigrationScenePartial:
		// 按对象名截断：schema、event 表及其主键/唯一约束/索引。
		b.execBaseline(t, ctx,
			postgresschema.Qualified(b.schema, "event_type_position_idx"))
	case eventtest.MigrationSceneConflict:
		b.createConflict(t, ctx)
	default:
		t.Fatalf("unknown migration scene %d", int(scene))
	}
}

// execBaseline 顺序执行计划语句（与期望对象按序配对）；lastObject 非空时
// 执行到该对象为止。
func (b *suiteBackend) execBaseline(t *testing.T, ctx context.Context, lastObject string) {
	t.Helper()
	paired, err := postgresschema.PlannedStatements(b.schema)
	if err != nil {
		t.Fatalf("planned statements: %v", err)
	}
	for i, ps := range paired {
		if _, err := b.pool.Exec(ctx, ps.SQL); err != nil {
			t.Fatalf("prepare statement %d: %v\n%s", i, err, ps.SQL)
		}
		if lastObject != "" && ps.Object.Name == lastObject {
			return
		}
	}
	if lastObject != "" {
		t.Fatalf("partial baseline stop object %q not found", lastObject)
	}
}

// createConflict 用 postgresschema.EventColumns 派生建表列，只把 payload
// 改成 text 制造定义冲突；其余列定义随基线变更自动跟随。
func (b *suiteBackend) createConflict(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := b.pool.Exec(ctx, "CREATE SCHEMA "+b.schema); err != nil {
		t.Fatalf("prepare conflict scene: create schema: %v", err)
	}
	columns := postgresschema.EventColumns()
	for i := range columns {
		if columns[i].Name == "payload" {
			columns[i].DataType = "text"
		}
	}
	if _, err := b.pool.Exec(ctx,
		conflictEventTableDDL(b.schema, postgresschema.TableEvent, columns)); err != nil {
		t.Fatalf("prepare conflict scene: create conflicting event table: %v", err)
	}
}

func conflictEventTableDDL(schema, table string, columns []postgresschema.Column) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE %s.%s (\n", schema, table)
	for _, col := range columns {
		dataType := col.DataType
		if col.Sequence {
			dataType = "bigserial"
		}
		fmt.Fprintf(&sb, "\t%s %s", col.Name, dataType)
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		sb.WriteString(",\n")
	}
	sb.WriteString("\tCONSTRAINT event_pk PRIMARY KEY (id)\n);")
	return sb.String()
}

func (b *suiteBackend) Reset(t *testing.T) {
	t.Helper()
	b.clearFault()
	b.drop()
	b.schema = newSuiteSchema(t)
	t.Cleanup(func() { b.drop() })
}

// InjectFault 把语句边界故障映射到执行器钩子：
//   - FaultBeforeStatement：首条语句执行前返回错误；
//   - FaultAfterStatement：首条语句成功执行后（第二条语句前）返回错误。
//
// 其余故障点不支持。
func (b *suiteBackend) InjectFault(point event.FaultPoint, err error) error {
	b.clearFault()
	if err == nil {
		return nil
	}
	switch point {
	case event.FaultBeforeStatement, event.FaultAfterStatement:
	default:
		return eventtest.ErrUnsupportedByBackend
	}
	calls := 0
	b.restoreHook = pgxc.SetStatementHookForTest(func(postgresschema.PlannedStatement) error {
		calls++
		if point == event.FaultBeforeStatement {
			return err
		}
		if calls == 2 {
			return err
		}
		return nil
	})
	return nil
}

func (b *suiteBackend) clearFault() {
	if b.restoreHook != nil {
		b.restoreHook()
		b.restoreHook = nil
	}
}

func (b *suiteBackend) Cleanup(t *testing.T) {
	t.Helper()
	b.clearFault()
	b.drop()
}

func (b *suiteBackend) drop() {
	if b.schema == "" {
		return
	}
	_, _ = b.pool.Exec(context.Background(),
		"DROP SCHEMA IF EXISTS "+b.schema+" CASCADE")
}

// newSuiteSchema 生成带随机后缀的 schema 名，避免并发/多包撞名。
func newSuiteSchema(t *testing.T) string {
	t.Helper()
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("random schema suffix: %v", err)
	}
	return fmt.Sprintf("go_event_suite_%d_%s",
		time.Now().UnixNano(), hex.EncodeToString(suffix[:]))
}
