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
// 时整体跳过；schema 名带随机后缀，须以 -p 1 串行执行。
func TestMigrationSuite(t *testing.T) {
	eventtest.RunMigrationSuite(t, &suiteBackend{pool: migrationPool(t)})
}

// suiteBackend 是 pgxc 的 MigrationBackend 适配器：每个子测试使用独立
// schema；支持快速路径与 before/after step 故障注入（映射到既有 step 钩子）。
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

// execBaseline 顺序执行基线步骤；lastObject 非空时执行到该对象为止。
func (b *suiteBackend) execBaseline(t *testing.T, ctx context.Context, lastObject string) {
	t.Helper()
	steps, err := postgresschema.Baseline(b.schema)
	if err != nil {
		t.Fatalf("baseline steps: %v", err)
	}
	for i, step := range steps {
		if _, err := b.pool.Exec(ctx, step.Statement); err != nil {
			t.Fatalf("prepare statement %d: %v\n%s", i, err, step.Statement)
		}
		if lastObject != "" && step.Object == lastObject {
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

// ForcePath：真实后端默认即 fast，显式切 fast 是成功的 no-op；step 不支持。
func (b *suiteBackend) ForcePath(path event.MigrationPath) error {
	if path == event.MigrationPathFast {
		return nil
	}
	return eventtest.ErrUnsupportedByBackend
}

// InjectFault 把 before/after step 映射到既有 step 钩子：
//   - FaultBeforeStep：首个 step 执行前返回错误；
//   - FaultAfterStep：首个 step 成功执行后（第二个 step 前）返回错误。
//
// 其余故障点不支持。
func (b *suiteBackend) InjectFault(point event.FaultPoint, err error) error {
	b.clearFault()
	if err == nil {
		return nil
	}
	switch point {
	case event.FaultBeforeStep, event.FaultAfterStep:
	default:
		return eventtest.ErrUnsupportedByBackend
	}
	calls := 0
	b.restoreHook = pgxc.SetStepHookForTest(func(postgresschema.Step) error {
		calls++
		if point == event.FaultBeforeStep {
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
