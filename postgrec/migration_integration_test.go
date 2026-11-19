package postgrec_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 驱动名 "pgx"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgrec"
	"github.com/jninng/go_event/postgresschema"
)

// 迁移集成测试需真实 Postgres：设置 TEST_POSTGRES_DSN 启用，未设置时跳过。
func migrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skip Postgres migration integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	return db
}

// newMigrationSchema 返回独立 schema 名并注册清理（隔离性）。
func newMigrationSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	schema := fmt.Sprintf("go_event_mig_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
	})
	return schema
}

func provisionBaseline(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	stmts, err := postgrec.Statements(postgrec.WithSchema(schema))
	if err != nil {
		t.Fatalf("statements: %v", err)
	}
	ctx := context.Background()
	for i, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("provision statement %d: %v\n%s", i, err, stmt)
		}
	}
}

func relationExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	var reg sql.NullString
	if err := db.QueryRow("SELECT to_regclass($1)::text", schema+"."+name).Scan(&reg); err != nil {
		t.Fatalf("to_regclass(%s.%s): %v", schema, name, err)
	}
	return reg.Valid && reg.String != ""
}

func schemaExists(t *testing.T, db *sql.DB, schema string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
		schema).Scan(&exists); err != nil {
		t.Fatalf("schema existence: %v", err)
	}
	return exists
}

func appendEvent(t *testing.T, drv event.Driver, typ, id string) {
	t.Helper()
	err := drv.Append(context.Background(), event.RawEnvelope{
		ID:         id,
		Type:       typ,
		Payload:    []byte(`{}`),
		OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}

// exerciseDML 跑一轮完整 DML：Append/Fetch/Claim/Heartbeat/Complete/Fail。
func exerciseDML(t *testing.T, drv event.Driver, typ, prefix string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()

	appendEvent(t, drv, typ, prefix+"-1")
	envs, err := drv.Fetch(ctx, event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(envs) == 0 {
		t.Fatal("fetch returned no events")
	}

	claim, err := drv.Claim(ctx, event.ClaimQuery{
		Type: typ, ConsumerType: prefix + "-c1",
		Lease: time.Minute, Limit: 10, RetryLimit: 3, Now: now,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Leases) != 1 {
		t.Fatalf("claim leases = %d, want 1", len(claim.Leases))
	}
	lease := claim.Leases[0]
	if err := drv.Heartbeat(ctx, event.HeartbeatRequest{
		EventID: lease.Envelope.ID, ConsumerType: lease.ConsumerType,
		LeaseToken: lease.LeaseToken, Extend: time.Minute, Now: now,
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := drv.Complete(ctx, event.CompleteRequest{
		EventID: lease.Envelope.ID, ConsumerType: lease.ConsumerType,
		LeaseToken: lease.LeaseToken, Now: now,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	appendEvent(t, drv, typ, prefix+"-2")
	claim, err = drv.Claim(ctx, event.ClaimQuery{
		Type: typ, ConsumerType: prefix + "-c2",
		Lease: time.Minute, Limit: 10, RetryLimit: 1, Now: now,
	})
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}
	if len(claim.Leases) == 0 {
		t.Fatal("second claim returned no leases")
	}
	if err := drv.Fail(ctx, event.FailRequest{
		EventID: claim.Leases[0].Envelope.ID, ConsumerType: claim.Leases[0].ConsumerType,
		LeaseToken: claim.Leases[0].LeaseToken, RetryLimit: 1,
		Backoff: event.LinearBackoff(time.Second, time.Second), Now: now,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
}

// requireIssue 断言报告包含指定对象的问题，并校验 Kind/Problem/Action。
func requireIssue(t *testing.T, report event.VerifyReport, object string,
	kind event.ObjectKind, problem event.VerifyProblem) {
	t.Helper()
	for _, issue := range report.Issues {
		if issue.Object != object {
			continue
		}
		if issue.Kind != kind {
			t.Fatalf("issue %s kind = %q, want %q", object, issue.Kind, kind)
		}
		if issue.Problem != problem {
			t.Fatalf("issue %s problem = %q, want %q", object, issue.Problem, problem)
		}
		if issue.Action == "" {
			t.Fatalf("issue %s action is empty", object)
		}
		return
	}
	t.Fatalf("issue %s not found in %+v", object, report.Issues)
}

// SQL 导出的手工脚本整段可重放：空库以简单协议整批执行（等价 psql 整段
// 执行，脚本自带 BEGIN/COMMIT）两次，均可成功且执行后 Verify 通过。
func TestSQLScriptReplayable(t *testing.T) {
	pool := migrationDB(t)
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	scriptDB, err := sql.Open("pgx", dsn+sep+"default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("open script db: %v", err)
	}
	t.Cleanup(func() { _ = scriptDB.Close() })

	schema := newMigrationSchema(t, pool)
	script, err := postgrec.SQL(postgrec.WithSchema(schema))
	if err != nil {
		t.Fatalf("sql: %v", err)
	}
	ctx := context.Background()
	for pass := 1; pass <= 2; pass++ {
		if _, err := scriptDB.ExecContext(ctx, script); err != nil {
			t.Fatalf("execute script pass %d: %v", pass, err)
		}
	}
	report, err := postgrec.Verify(ctx, pool, postgrec.WithSchema(schema))
	if err != nil {
		t.Fatalf("verify after script: %v", err)
	}
	if !report.OK() {
		t.Fatalf("verify issues after script: %+v", report.Issues)
	}
	if !relationExists(t, pool, schema, "event_claim") {
		t.Fatal("event_claim table missing after script execution")
	}
}

func TestCreateOnEmptyDatabase(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("FreshSchema_Migrate_ConvergesAndVerifies", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !report.OK() {
			t.Fatalf("verify issues: %+v", report.Issues)
		}
	})
	t.Run("FreshSchema_Migrate_CreatesBothTables", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if !relationExists(t, db, schema, "event") {
			t.Fatal("event table missing after migrate")
		}
		if !relationExists(t, db, schema, "event_claim") {
			t.Fatal("event_claim table missing after migrate")
		}
	})
	t.Run("FreshSchema_CreateDriver_AppendSucceeds", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
		appendEvent(t, drv, "test.migration", "empty-db-1")
	})
}

func TestMigrateIdempotentOnCompleteBaseline(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("CompleteBaseline_Migrate_NoError", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate over complete baseline: %v", err)
		}
	})
	t.Run("CompleteBaseline_MigrateTwice_Idempotent", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 1: %v", err)
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 2: %v", err)
		}
	})
	t.Run("CompleteBaseline_Verify_ReportsNoIssues", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !report.OK() {
			t.Fatalf("verify issues: %+v", report.Issues)
		}
	})
}

func TestMigrateFillsPartialBaseline(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("PartialBaseline_Migrate_FillsMissingObjects", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		// 只建 schema 与 event 裸表（约束与索引留给 Migrate 补齐），模拟部分存量。
		stmts, err := postgrec.Statements(postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("statements: %v", err)
		}
		for i := 0; i < 2; i++ {
			if _, err := db.ExecContext(ctx, stmts[i]); err != nil {
				t.Fatalf("provision statement %d: %v", i, err)
			}
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate over partial baseline: %v", err)
		}
		if !relationExists(t, db, schema, "event_claim") {
			t.Fatal("event_claim table not filled")
		}
	})
	t.Run("PartialBaseline_Verify_ReportsNoIssues", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		stmts, err := postgrec.Statements(postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("statements: %v", err)
		}
		for i := 0; i < 2; i++ {
			if _, err := db.ExecContext(ctx, stmts[i]); err != nil {
				t.Fatalf("provision statement %d: %v", i, err)
			}
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !report.OK() {
			t.Fatalf("verify issues: %+v", report.Issues)
		}
	})
}

func TestMigrateRepairsMissingConstraints(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("MissingConstraints_Migrate_ReAddsAndConverges", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		// 依赖顺序：先 FK，再 PK，最后 UNIQUE。
		for _, stmt := range []string{
			fmt.Sprintf(`ALTER TABLE %s.event_claim DROP CONSTRAINT event_claim_event_id_fkey`, schema),
			fmt.Sprintf(`ALTER TABLE %s.event_claim DROP CONSTRAINT event_claim_pk`, schema),
			fmt.Sprintf(`ALTER TABLE %s.event DROP CONSTRAINT event_pk`, schema),
			fmt.Sprintf(`ALTER TABLE %s.event DROP CONSTRAINT event_type_id_uniq`, schema),
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("drop constraint: %v\n%s", err, stmt)
			}
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate after dropping constraints: %v", err)
		}
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !report.OK() {
			t.Fatalf("verify issues after repair: %+v", report.Issues)
		}
	})
}

func TestMigrateDefinitionConflict(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	conflicting := func(t *testing.T) string {
		t.Helper()
		schema := newMigrationSchema(t, db)
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		// payload 类型与基线（bytea）冲突。
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %s.event (
	id          text        NOT NULL,
	type        text        NOT NULL,
	payload     text        NOT NULL,
	metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
	occurred_at timestamptz NOT NULL,
	position    bigserial   NOT NULL,
	CONSTRAINT event_pk PRIMARY KEY (id)
)`, schema)); err != nil {
			t.Fatalf("create conflicting table: %v", err)
		}
		return schema
	}

	t.Run("ConflictingColumn_Migrate_ReturnsVerifyError", func(t *testing.T) {
		schema := conflicting(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
	})
	t.Run("ConflictingColumn_Migrate_ReportsOutdatedPayload", func(t *testing.T) {
		schema := conflicting(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		requireIssue(t, verifyErr.Report, schema+".event.payload",
			event.KindColumn, event.ProblemOutdated)
	})
	t.Run("ConflictingColumn_Migrate_DoesNotCreateMissingTable", func(t *testing.T) {
		schema := conflicting(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		if relationExists(t, db, schema, "event_claim") {
			t.Fatal("event_claim must not be created when definitions conflict")
		}
	})
	t.Run("ConflictingColumn_Migrate_DoesNotAlterExistingTable", func(t *testing.T) {
		schema := conflicting(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		var dataType string
		if err := db.QueryRowContext(ctx, `
SELECT data_type FROM information_schema.columns
WHERE table_schema = $1 AND table_name = 'event' AND column_name = 'payload'`,
			schema).Scan(&dataType); err != nil {
			t.Fatalf("read payload type: %v", err)
		}
		if dataType != "text" {
			t.Fatalf("payload type = %q, want untouched text", dataType)
		}
	})
}

func TestMigrateConstraintAndIndexConflicts(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("UniqueColumnsConflict_Migrate_RejectedAsOutdated", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		for _, stmt := range []string{
			fmt.Sprintf(`ALTER TABLE %s.event DROP CONSTRAINT event_type_id_uniq`, schema),
			fmt.Sprintf(`ALTER TABLE %s.event ADD CONSTRAINT event_type_id_uniq UNIQUE (type)`, schema),
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("prepare unique conflict: %v\n%s", err, stmt)
			}
		}
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		requireIssue(t, verifyErr.Report, schema+".event_type_id_uniq",
			event.KindConstraint, event.ProblemOutdated)
	})
	t.Run("IndexColumnsConflict_Migrate_RejectedAsOutdated", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		for _, stmt := range []string{
			fmt.Sprintf(`DROP INDEX %s.event_type_position_idx`, schema),
			fmt.Sprintf(`CREATE INDEX event_type_position_idx ON %s.event (position)`, schema),
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("prepare index conflict: %v\n%s", err, stmt)
			}
		}
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		requireIssue(t, verifyErr.Report, schema+".event_type_position_idx",
			event.KindIndex, event.ProblemOutdated)
	})
	t.Run("IndexOnOtherTable_Migrate_RejectedAsOutdated", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		for _, stmt := range []string{
			fmt.Sprintf(`DROP INDEX %s.event_type_position_idx`, schema),
			fmt.Sprintf(`CREATE INDEX event_type_position_idx ON %s.event_claim (consumer_type)`, schema),
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("prepare cross-table index: %v\n%s", err, stmt)
			}
		}
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		requireIssue(t, verifyErr.Report, schema+".event_type_position_idx",
			event.KindIndex, event.ProblemOutdated)
		var table string
		if err := db.QueryRowContext(ctx, `
SELECT tablename FROM pg_indexes WHERE schemaname = $1 AND indexname = 'event_type_position_idx'`,
			schema).Scan(&table); err != nil {
			t.Fatalf("read conflicting index: %v", err)
		}
		if table != "event_claim" {
			t.Fatalf("conflicting index table = %q, want untouched event_claim", table)
		}
	})
	t.Run("MissingSequenceDefault_Migrate_RejectedAsOutdated", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`ALTER TABLE %s.event ALTER COLUMN position DROP DEFAULT`, schema)); err != nil {
			t.Fatalf("drop position default: %v", err)
		}
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("verify = %v, want *VerifyError", err)
		}
		requireIssue(t, report, schema+".event.position", event.KindColumn, event.ProblemOutdated)

		err = postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError (baseline must not silently accept)", err)
		}
		var defaultCount int
		if err := db.QueryRowContext(ctx, `
SELECT count(*) FROM pg_attrdef d
JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = 'event' AND a.attname = 'position'`,
			schema).Scan(&defaultCount); err != nil {
			t.Fatalf("read position default: %v", err)
		}
		if defaultCount != 0 {
			t.Fatalf("position default count = %d, want 0 (not silently repaired)", defaultCount)
		}
	})
}

func TestVerifyMissingObjects(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	assertAllMissing := func(t *testing.T, report event.VerifyReport, schema string) {
		t.Helper()
		requireIssue(t, report, schema, event.KindSchema, event.ProblemMissing)
		requireIssue(t, report, schema+".event", event.KindTable, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim", event.KindTable, event.ProblemMissing)
		requireIssue(t, report, schema+".event_pk", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_type_id_uniq", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_pk", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_event_id_fkey", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_type_position_idx", event.KindIndex, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_eligible_idx", event.KindIndex, event.ProblemMissing)
	}

	t.Run("SchemaMissing_Verify_ReportsEveryObjectMissing", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("verify = %v, want *VerifyError", err)
		}
		assertAllMissing(t, report, schema)
	})
	t.Run("TablesMissing_Verify_ReportsEveryObjectMissingExceptSchema", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("verify = %v, want *VerifyError", err)
		}
		for _, issue := range report.Issues {
			if issue.Kind == event.KindSchema {
				t.Fatalf("unexpected schema issue: %+v", issue)
			}
		}
		requireIssue(t, report, schema+".event", event.KindTable, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim", event.KindTable, event.ProblemMissing)
		requireIssue(t, report, schema+".event_pk", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_type_id_uniq", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_pk", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_event_id_fkey", event.KindConstraint, event.ProblemMissing)
		requireIssue(t, report, schema+".event_type_position_idx", event.KindIndex, event.ProblemMissing)
		requireIssue(t, report, schema+".event_claim_eligible_idx", event.KindIndex, event.ProblemMissing)
	})
}

func TestNoneModeDoesNotMigrate(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("MissingSchema_NoneModeAppend_Fails", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateNone))
		err := drv.Append(ctx, event.RawEnvelope{
			ID: "none-1", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		if err == nil {
			t.Fatal("append = nil, want error without tables")
		}
	})
	t.Run("MissingSchema_NoneMode_DoesNotCreateTables", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateNone))
		err := drv.Append(ctx, event.RawEnvelope{
			ID: "none-2", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		if err == nil {
			t.Fatal("append = nil, want error without tables")
		}
		if schemaExists(t, db, schema) || relationExists(t, db, schema, "event") {
			t.Fatal("None mode must not create schema or tables")
		}
	})
}

// 迁移锁语义：持锁期间有界超时且不建对象；释放后成功；事务级锁随提交释放。
func TestMigrationLockTimeoutAndRelease(t *testing.T) {
	db := migrationDB(t)
	restoreTimeout := postgrec.SetLockWaitTimeoutForTest(300 * time.Millisecond)
	defer restoreTimeout()
	schema := newMigrationSchema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "go_event:" + schema

	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire holder connection: %v", err)
	}
	unlocked := false
	// 解锁与关闭同一清理：先解锁再 Close，避免连接被提前归还/关闭。
	t.Cleanup(func() {
		if !unlocked {
			_, _ = holder.ExecContext(context.Background(),
				"SELECT pg_advisory_unlock(hashtext($1))", key)
		}
		_ = holder.Close()
	})
	if _, err := holder.ExecContext(ctx,
		"SELECT pg_advisory_lock(hashtext($1))", key); err != nil {
		t.Fatalf("hold migration lock: %v", err)
	}

	start := time.Now()
	err = postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
	elapsed := time.Since(start)
	if !errors.Is(err, event.ErrMigrationLockTimeout) {
		t.Fatalf("migrate with held lock = %v, want ErrMigrationLockTimeout", err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("lock wait = %v, want bounded under 2s", elapsed)
	}
	if schemaExists(t, db, schema) || relationExists(t, db, schema, "event") {
		t.Fatal("objects must not be created while the lock is held")
	}

	if _, err := holder.ExecContext(ctx,
		"SELECT pg_advisory_unlock(hashtext($1))", key); err != nil {
		t.Fatalf("release migration lock: %v", err)
	}
	unlocked = true

	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate after lock release: %v", err)
	}
	var acquired bool
	if err := db.QueryRowContext(ctx,
		"SELECT pg_try_advisory_xact_lock(hashtext($1))", key).Scan(&acquired); err != nil {
		t.Fatalf("probe transaction lock: %v", err)
	}
	if !acquired {
		t.Fatal("transaction-level lock must be released on commit")
	}
}

// 中途失败（同名 enum 类型令 CREATE TABLE event_claim 失败）：此前所有
// 语句已在同一事务内执行，必须整体回滚。
func TestMigrationMidwayFailureRollback(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	conflictingType := func(t *testing.T) string {
		t.Helper()
		schema := newMigrationSchema(t, db)
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		// 同名类型令迁移在建 event_claim 时中途失败：event 表与其索引已在
		// 同一事务内先行建出，失败后必须整体回滚。
		if _, err := db.ExecContext(ctx,
			"CREATE TYPE "+schema+".event_claim AS ENUM ('x')"); err != nil {
			t.Fatalf("create conflicting type: %v", err)
		}
		return schema
	}

	t.Run("MidwayFailure_Migrate_ReturnsVerifyError", func(t *testing.T) {
		schema := conflictingType(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
	})
	t.Run("MidwayFailure_Migrate_RollsBackCreatedTables", func(t *testing.T) {
		schema := conflictingType(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		if relationExists(t, db, schema, "event") {
			t.Fatal("event table must be rolled back")
		}
		if relationExists(t, db, schema, "event_claim") {
			t.Fatal("event_claim relation must not exist")
		}
	})
	t.Run("MidwayFailure_Migrate_LeavesConflictingType", func(t *testing.T) {
		schema := conflictingType(t)
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		var exists bool
		if err := db.QueryRowContext(ctx, `
SELECT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
               WHERE n.nspname = $1 AND t.typname = 'event_claim')`, schema).Scan(&exists); err != nil {
			t.Fatalf("read type: %v", err)
		}
		if !exists {
			t.Fatal("conflicting enum type must remain untouched")
		}
	})
}

// 最后一条语句失败（注入）：此前所有语句已执行，事务必须整体回滚。
func TestMigrationLastStatementRollback(t *testing.T) {
	db := migrationDB(t)
	schema := newMigrationSchema(t, db)
	ctx := context.Background()
	injected := errors.New("injected last-statement failure")
	restore := postgrec.SetStatementHookForTest(func(stmt postgresschema.PlannedStatement) error {
		if stmt.Object.Name == schema+".event_claim_eligible_idx" {
			return injected
		}
		return nil
	})
	defer restore()

	err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
	if !errors.Is(err, injected) {
		t.Fatalf("migrate = %v, want injected failure", err)
	}
	if schemaExists(t, db, schema) {
		t.Fatal("schema must be rolled back on last-statement failure")
	}
	if relationExists(t, db, schema, "event") || relationExists(t, db, schema, "event_claim") {
		t.Fatal("tables must be rolled back on last-statement failure")
	}
}

func TestVerifyModeDriver(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("MissingTables_VerifyModeAppend_ReturnsVerifyError", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateVerify))
		err := drv.Append(ctx, event.RawEnvelope{
			ID: "verify-1", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("append = %v, want *VerifyError", err)
		}
		if schemaExists(t, db, schema) {
			t.Fatal("Verify mode must not create the schema")
		}
	})
	t.Run("CompleteSchema_VerifyModeAppend_Succeeds", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionBaseline(t, db, schema)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema),
			postgrec.WithMigrateMode(event.MigrateVerify))
		appendEvent(t, drv, "test.migration", "verify-2")
	})
}

// DML-only 连接是可选集成用例：设置 TEST_DML_ONLY_DSN 时运行，否则跳过。
func TestDMLOnlyConnection(t *testing.T) {
	dsn := os.Getenv("TEST_DML_ONLY_DSN")
	if dsn == "" {
		t.Skip("TEST_DML_ONLY_DSN not set; skip DML-only connection test")
	}
	admin := migrationDB(t)
	dmlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open dml db: %v", err)
	}
	t.Cleanup(func() { _ = dmlDB.Close() })
	ctx := context.Background()
	if err := dmlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping dml db: %v", err)
	}
	schema := newMigrationSchema(t, admin)
	if err := postgrec.Migrate(ctx, admin, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("provision schema: %v", err)
	}

	var role string
	if err := dmlDB.QueryRowContext(ctx, "SELECT current_user").Scan(&role); err != nil {
		t.Fatalf("read current_user: %v", err)
	}
	quotedRole := `"` + strings.ReplaceAll(role, `"`, `""`) + `"`
	grant := fmt.Sprintf(`
GRANT USAGE ON SCHEMA %[1]s TO %[2]s;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %[1]s TO %[2]s;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %[1]s TO %[2]s;`, schema, quotedRole)
	if _, err := admin.ExecContext(ctx, grant); err != nil {
		t.Skipf("cannot grant privileges to DML role: %v", err)
	}

	for _, mode := range []event.MigrateMode{event.MigrateVerify, event.MigrateNone} {
		t.Run(mode.String()+"Mode_DMLConnection_FullDMLCycle", func(t *testing.T) {
			drv := postgrec.NewDriver(dmlDB, postgrec.WithSchema(schema),
				postgrec.WithMigrateMode(mode))
			exerciseDML(t, drv, "dml."+mode.String(), "dml-"+mode.String())
		})
	}
	t.Run("CreateMode_DMLConnection_ReportsPermissionDenied", func(t *testing.T) {
		fresh := newMigrationSchema(t, admin)
		drv := postgrec.NewDriver(dmlDB, postgrec.WithSchema(fresh),
			postgrec.WithMigrateMode(event.MigrateCreate))
		err := drv.Append(ctx, event.RawEnvelope{
			ID: "dml-4", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		if !errors.Is(err, event.ErrMigrationPermissionDenied) {
			t.Fatalf("DML role unexpectedly allowed DDL: %v", err)
		}
		if !event.IsPermanentMigrationError(err) {
			t.Fatal("permission error must be classified permanent (bounded retry)")
		}
		// 永久错误 fail-fast：再次调用不重新执行 DDL。
		err2 := drv.Append(ctx, event.RawEnvelope{
			ID: "dml-5", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		if !errors.Is(err2, event.ErrMigrationPermissionDenied) {
			t.Fatalf("second append = %v, want same permanent error", err2)
		}
	})
}

// 迁移锁必须在第一条 DDL 之前取得：在首条语句钩子里另开连接探测必须失败。
func TestLockAcquiredBeforeFirstDDL_ProbeFails(t *testing.T) {
	db := migrationDB(t)
	schema := newMigrationSchema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := postgresschema.LockKey(schema)

	var (
		probed       bool
		probeAcquire bool
		probeErr     error
	)
	restore := postgrec.SetStatementHookForTest(func(postgresschema.PlannedStatement) error {
		if probed {
			return nil
		}
		probed = true
		probeErr = db.QueryRowContext(ctx,
			"SELECT pg_try_advisory_xact_lock(hashtext($1))", key).Scan(&probeAcquire)
		return probeErr
	})
	defer restore()

	if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if !probed {
		t.Fatal("statement hook did not run")
	}
	if probeErr != nil {
		t.Fatalf("probe lock: %v", probeErr)
	}
	if probeAcquire {
		t.Fatal("migration lock must be held before the first DDL statement")
	}
}

// 并发首启正向用例：两个 Driver 同时首次操作都应成功且对象唯一。
func TestConcurrentFirstUse(t *testing.T) {
	db := migrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runConcurrent := func(t *testing.T) (string, []error) {
		t.Helper()
		schema := newMigrationSchema(t, db)
		drivers := []event.Driver{
			postgrec.NewDriver(db, postgrec.WithSchema(schema)),
			postgrec.NewDriver(db, postgrec.WithSchema(schema)),
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, len(drivers))
		for i := range drivers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = drivers[i].Append(ctx, event.RawEnvelope{
					ID:         fmt.Sprintf("concurrent-%d", i),
					Type:       "concurrent",
					Payload:    []byte(`{}`),
					OccurredAt: time.Now(),
				})
			}(i)
		}
		close(start)
		wg.Wait()
		return schema, errs
	}
	requireNoErrors := func(t *testing.T, errs []error) {
		t.Helper()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("driver %d append = %v, want nil", i, err)
			}
		}
	}

	t.Run("TwoDrivers_ConcurrentFirstAppend_BothSucceed", func(t *testing.T) {
		_, errs := runConcurrent(t)
		requireNoErrors(t, errs)
	})
	t.Run("TwoDrivers_ConcurrentFirstUse_VerifyReportsNoIssues", func(t *testing.T) {
		schema, errs := runConcurrent(t)
		requireNoErrors(t, errs)
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !report.OK() {
			t.Fatalf("verify issues: %+v", report.Issues)
		}
	})
	t.Run("TwoDrivers_ConcurrentFirstUse_CreatesSingleObjectSet", func(t *testing.T) {
		schema, errs := runConcurrent(t)
		requireNoErrors(t, errs)
		var count int
		if err := db.QueryRowContext(ctx, `
SELECT count(*) FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname IN
	('event', 'event_claim', 'event_type_position_idx', 'event_claim_eligible_idx')`,
			schema).Scan(&count); err != nil {
			t.Fatalf("count baseline relations: %v", err)
		}
		if count != 4 {
			t.Fatalf("baseline relation count = %d, want 4", count)
		}
	})
}

// 真实 23505：存量表无 PK 且 id 重复，加 PK 触发唯一冲突，必须 fail-fast。
func TestMigrationUniqueViolation(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("DuplicateIDsWithoutPK_Migrate_ReturnsOutdatedVerifyError", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %s.event (
	id          text        NOT NULL,
	type        text        NOT NULL,
	payload     bytea       NOT NULL,
	metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
	occurred_at timestamptz NOT NULL,
	position    bigserial   NOT NULL
)`, schema)); err != nil {
			t.Fatalf("create legacy event table: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s.event (id, type, payload, occurred_at)
VALUES ('dup', 'a', ''::bytea, now()), ('dup', 'b', ''::bytea, now())`, schema)); err != nil {
			t.Fatalf("insert duplicate ids: %v", err)
		}

		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		requireIssue(t, verifyErr.Report, schema+".event_pk",
			event.KindConstraint, event.ProblemOutdated)
		if relationExists(t, db, schema, "event_claim") {
			t.Fatal("event_claim must not be created after duplicate-key failure")
		}
		var duplicates int
		if err := db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s.event WHERE id = 'dup'", schema)).Scan(&duplicates); err != nil {
			t.Fatalf("count duplicates: %v", err)
		}
		if duplicates != 2 {
			t.Fatalf("duplicate rows = %d, want untouched 2", duplicates)
		}
	})
}

// 已取消 ctx：Migrate 与首次 Append 都返回 context.Canceled 且不建对象。
func TestCanceledContext(t *testing.T) {
	db := migrationDB(t)

	t.Run("Canceled_Migrate_ReturnsContextCanceledWithoutObjects", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("migrate = %v, want context.Canceled", err)
		}
		if schemaExists(t, db, schema) {
			t.Fatal("canceled migrate must not create objects")
		}
	})
	t.Run("Canceled_FirstAppend_ReturnsContextCanceled", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
		err := drv.Append(ctx, event.RawEnvelope{
			ID: "canceled-1", Type: "t", Payload: []byte(`{}`), OccurredAt: time.Now(),
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("append = %v, want context.Canceled", err)
		}
		if schemaExists(t, db, schema) {
			t.Fatal("canceled first append must not create objects")
		}
	})
}

// provisionVersion 顺序执行指定版本的计划语句（模拟旧二进制创建的存量
// 基线，供升级路径用例）。
func provisionVersion(t *testing.T, db *sql.DB, schema string, version int) {
	t.Helper()
	plan, err := postgresschema.Plan(schema)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if version < 1 || version > len(plan) {
		t.Fatalf("version %d out of plan range", version)
	}
	ctx := context.Background()
	for i, stmt := range plan[version-1].Statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("provision v%d statement %d: %v\n%s", version, i, err, stmt)
		}
	}
}

func columnExists(t *testing.T, db *sql.DB, schema, table, column string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)`,
		schema, table, column).Scan(&exists); err != nil {
		t.Fatalf("column exists %s.%s.%s: %v", schema, table, column, err)
	}
	return exists
}

// 旧二进制（v1 基线）升级：Migrate 幂等补齐 v2 加列（last_error /
// failed_at），升级随部署隐式发生（ADR 0007）；Verify 对 v1 基线报告
// 缺失列（待升级）。
func TestMigrateUpgradesV1Baseline(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("V1Baseline_Migrate_AddsErrorSummaryColumns", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate over v1 baseline: %v", err)
		}
		if !columnExists(t, db, schema, "event_claim", "last_error") ||
			!columnExists(t, db, schema, "event_claim", "failed_at") {
			t.Fatal("v2 columns not added to event_claim")
		}
	})
	t.Run("V1Baseline_MigrateTwice_Idempotent", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 1: %v", err)
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 2: %v", err)
		}
	})
	t.Run("V1Baseline_Verify_ReportsMissingVersionColumns", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("verify on v1 baseline = %v, want *VerifyError (missing columns)", err)
		}
		requireIssue(t, report,
			postgresschema.Qualified(schema, "event_claim")+".last_error",
			event.KindColumn, event.ProblemMissing)
		requireIssue(t, report,
			postgresschema.Qualified(schema, "event_claim")+".failed_at",
			event.KindColumn, event.ProblemMissing)
	})
	t.Run("V1Baseline_DriverFirstAppend_UpgradeConverges", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
		exerciseDML(t, drv, "t.upgrade", "up1")
		if !columnExists(t, db, schema, "event_claim", "last_error") {
			t.Fatal("create-mode first use must upgrade v1 baseline with new columns")
		}
	})
}

// 旧二进制（v2）升级：Migrate 幂等补齐 v3 加列（event.deliver_at，延迟
// 事件 ADR 0013），升级随部署隐式发生（ADR 0007）；Verify 对 v2 基线
// 报告缺失列（待升级）。
func TestMigrateUpgradesV2Baseline(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()

	t.Run("V2Baseline_Migrate_AddsDeliverAtColumn", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		provisionVersion(t, db, schema, 2)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate over v2 baseline: %v", err)
		}
		if !columnExists(t, db, schema, "event", "deliver_at") {
			t.Fatal("v3 column deliver_at not added to event")
		}
	})
	t.Run("V2Baseline_MigrateTwice_Idempotent", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		provisionVersion(t, db, schema, 2)
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 1: %v", err)
		}
		if err := postgrec.Migrate(ctx, db, postgrec.WithSchema(schema)); err != nil {
			t.Fatalf("migrate 2: %v", err)
		}
	})
	t.Run("V2Baseline_Verify_ReportsMissingDeliverAt", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		provisionVersion(t, db, schema, 2)
		report, err := postgrec.Verify(ctx, db, postgrec.WithSchema(schema))
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("verify on v2 baseline = %v, want *VerifyError (missing column)", err)
		}
		requireIssue(t, report,
			postgresschema.Qualified(schema, "event")+".deliver_at",
			event.KindColumn, event.ProblemMissing)
	})
	t.Run("V2Baseline_DriverFirstAppend_UpgradeConverges", func(t *testing.T) {
		schema := newMigrationSchema(t, db)
		provisionVersion(t, db, schema, 1)
		provisionVersion(t, db, schema, 2)
		drv := postgrec.NewDriver(db, postgrec.WithSchema(schema))
		exerciseDML(t, drv, "t.upgrade3", "up3")
		if !columnExists(t, db, schema, "event", "deliver_at") {
			t.Fatal("create-mode first use must upgrade v2 baseline with deliver_at")
		}
	})
}
