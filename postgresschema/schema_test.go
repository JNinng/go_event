package postgresschema_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

func TestValidateSchema(t *testing.T) {
	valid := []string{"public", "s", "a_1", "_x", "Schema9", "go_event_123"}
	for _, name := range valid {
		t.Run("ValidName_"+name+"_Accepted", func(t *testing.T) {
			if err := postgresschema.ValidateSchema(name); err != nil {
				t.Fatalf("ValidateSchema(%q) = %v, want nil", name, err)
			}
		})
	}
	invalid := []string{"", "1abc", "a-b", "a b", "public;drop table x", "a.b", "`x`", "x\ny"}
	for _, name := range invalid {
		t.Run("InvalidName_"+strings.ReplaceAll(name, " ", "_")+"_Rejected", func(t *testing.T) {
			if err := postgresschema.ValidateSchema(name); err == nil {
				t.Fatalf("ValidateSchema(%q) = nil, want error", name)
			}
		})
	}
}

func TestPlan(t *testing.T) {
	t.Run("InvalidSchema_Plan_ReturnsError", func(t *testing.T) {
		versions, err := postgresschema.Plan("bad-name")
		if err == nil {
			t.Fatal("Plan(invalid) = nil error, want error")
		}
		if versions != nil {
			t.Fatalf("Plan(invalid) returned %d versions, want nil", len(versions))
		}
		if _, err := postgresschema.PlanStatements("bad-name"); err == nil {
			t.Fatal("PlanStatements(invalid) = nil error, want error")
		}
		if _, err := postgresschema.PlannedStatements("bad-name"); err == nil {
			t.Fatal("PlannedStatements(invalid) = nil error, want error")
		}
	})
	t.Run("Default_Plan_VersionsStrictlyIncreasingWithStatements", func(t *testing.T) {
		plan, err := postgresschema.Plan("public")
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan) == 0 {
			t.Fatal("Plan = empty, want baseline v1")
		}
		prev := 0
		for _, v := range plan {
			if v.Number != prev+1 {
				t.Fatalf("version number %d after %d, want strictly increasing from 1", v.Number, prev)
			}
			if len(v.Statements) == 0 {
				t.Fatalf("version %d has no statements", v.Number)
			}
			prev = v.Number
		}
		if plan[0].Number != 1 {
			t.Fatalf("baseline version = %d, want 1", plan[0].Number)
		}
	})
	t.Run("Default_BaselineStatements_PairWithExpectedObjectsOneToOne", func(t *testing.T) {
		paired, err := postgresschema.PlannedStatements("public")
		if err != nil {
			t.Fatalf("PlannedStatements: %v", err)
		}
		if len(paired) != 12 {
			t.Fatalf("plan statements = %d, want 12 (baseline v1: 9 + v2 add-column: 2 + v3 add-column: 1)", len(paired))
		}
		objs := postgresschema.ExpectedObjects("public")
		if len(objs) != len(paired) {
			t.Fatalf("expected objects = %d, statements = %d, want 1:1", len(objs), len(paired))
		}
		// 每条语句的所属对象与配对期望对象一致；建表语句不含内联约束。
		for i, ps := range paired {
			if ps.SQL == "" {
				t.Fatalf("statement %d is empty", i)
			}
			if ps.Object.Kind == event.KindSchema {
				if ps.Object.Table != "" {
					t.Fatalf("schema statement %d has owning table %q", i, ps.Object.Table)
				}
			} else if ps.Object.Table == "" {
				t.Fatalf("statement %d pairs with object %s without owning table", i, ps.Object.Name)
			}
		}
		for _, ps := range paired {
			if strings.Contains(ps.SQL, "CONSTRAINT ") && strings.Contains(ps.SQL, "CREATE TABLE") {
				t.Fatalf("table DDL must not inline constraints (constraints go through DO blocks): %s", ps.SQL)
			}
		}
	})
	t.Run("VersionTwo_Statements_AddEventClaimColumnsIdempotent", func(t *testing.T) {
		plan, err := postgresschema.Plan("tenant")
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan) != 3 {
			t.Fatalf("plan versions = %d, want 3", len(plan))
		}
		v2 := plan[1]
		if v2.Number != 2 || len(v2.Statements) != 2 {
			t.Fatalf("v2 = %+v, want 2 add-column statements", v2)
		}
		// 一条 ALTER ... ADD COLUMN IF NOT EXISTS 恰好对应一个列对象。
		paired, err := postgresschema.PlannedStatements("tenant")
		if err != nil {
			t.Fatalf("PlannedStatements: %v", err)
		}
		want := []struct {
			name   string
			ddl    string
			column string
			typ    string
		}{
			{"tenant.event_claim.last_error", "ALTER TABLE tenant.event_claim ADD COLUMN IF NOT EXISTS last_error text;", "last_error", "text"},
			{"tenant.event_claim.failed_at", "ALTER TABLE tenant.event_claim ADD COLUMN IF NOT EXISTS failed_at timestamptz;", "failed_at", "timestamptz"},
		}
		for i, w := range want {
			ps := paired[9+i]
			if ps.SQL != w.ddl {
				t.Fatalf("v2 statement %d = %q, want %q", i, ps.SQL, w.ddl)
			}
			if ps.Object.Kind != event.KindColumn || ps.Object.Name != w.name {
				t.Fatalf("v2 statement %d pairs with %+v, want column object %s", i, ps.Object, w.name)
			}
			if len(ps.Object.Columns) != 1 || ps.Object.Columns[0].Name != w.column {
				t.Fatalf("column object %s must declare exactly column %s: %+v", w.name, w.column, ps.Object.Columns)
			}
			if ps.Object.Columns[0].NotNull {
				t.Fatalf("version column %s must be nullable (expand-compatible)", w.name)
			}
			if w.typ == "timestamptz" && ps.Object.Columns[0].DataType != "timestamp with time zone" {
				t.Fatalf("column %s expected type = %q, want format_type form", w.name, ps.Object.Columns[0].DataType)
			}
		}
		// 每条 v2 语句幂等：重复执行语义由 IF NOT EXISTS 保证（文本检查）。
		for _, stmt := range v2.Statements {
			if !strings.Contains(stmt, "ADD COLUMN IF NOT EXISTS") {
				t.Fatalf("v2 statement must be idempotent ADD COLUMN IF NOT EXISTS: %s", stmt)
			}
		}
	})
	t.Run("VersionThree_Statement_AddsEventDeliverAtIdempotent", func(t *testing.T) {
		plan, err := postgresschema.Plan("tenant")
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		v3 := plan[2]
		if v3.Number != 3 || len(v3.Statements) != 1 {
			t.Fatalf("v3 = %+v, want exactly 1 add-column statement", v3)
		}
		// 一条 ALTER ... ADD COLUMN IF NOT EXISTS 恰好对应一个列对象。
		paired, err := postgresschema.PlannedStatements("tenant")
		if err != nil {
			t.Fatalf("PlannedStatements: %v", err)
		}
		ps := paired[11]
		if ps.SQL != "ALTER TABLE tenant.event ADD COLUMN IF NOT EXISTS deliver_at timestamptz;" {
			t.Fatalf("v3 statement = %q", ps.SQL)
		}
		if ps.Object.Kind != event.KindColumn || ps.Object.Name != "tenant.event.deliver_at" {
			t.Fatalf("v3 statement pairs with %+v, want column object tenant.event.deliver_at", ps.Object)
		}
		if ps.Object.Table != "event" {
			t.Fatalf("v3 column owning table = %q, want event", ps.Object.Table)
		}
		if len(ps.Object.Columns) != 1 || ps.Object.Columns[0].Name != "deliver_at" {
			t.Fatalf("column object must declare exactly deliver_at: %+v", ps.Object.Columns)
		}
		if ps.Object.Columns[0].NotNull {
			t.Fatal("deliver_at must be nullable (expand-compatible, zero value = immediate)")
		}
		if ps.Object.Columns[0].DataType != "timestamp with time zone" {
			t.Fatalf("deliver_at type = %q, want format_type form", ps.Object.Columns[0].DataType)
		}
		if !strings.Contains(v3.Statements[0], "ADD COLUMN IF NOT EXISTS") {
			t.Fatalf("v3 statement must be idempotent ADD COLUMN IF NOT EXISTS: %s", v3.Statements[0])
		}
	})
}

func TestStatements(t *testing.T) {
	t.Run("InvalidSchema_Statements_ReturnsError", func(t *testing.T) {
		if _, err := postgresschema.Statements("bad-name"); err == nil {
			t.Fatal("Statements(invalid) = nil error, want error")
		}
	})
	t.Run("Default_Statements_ContainBaselineWithoutLock", func(t *testing.T) {
		stmts, err := postgresschema.Statements("tenant")
		if err != nil {
			t.Fatalf("Statements: %v", err)
		}
		joined := strings.Join(stmts, "\n")
		for _, want := range []string{
			"CREATE SCHEMA IF NOT EXISTS tenant",
			"CREATE TABLE IF NOT EXISTS tenant.event",
			"CREATE TABLE IF NOT EXISTS tenant.event_claim",
			"CREATE INDEX IF NOT EXISTS event_type_position_idx ON tenant.event",
			"CREATE INDEX IF NOT EXISTS event_claim_eligible_idx ON tenant.event_claim",
			"ADD CONSTRAINT event_pk PRIMARY KEY (id)",
			"ADD CONSTRAINT event_claim_pk PRIMARY KEY (event_id, consumer_type)",
			"event_claim_event_id_fkey",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("Statements missing %q", want)
			}
		}
		for _, stmt := range stmts {
			if strings.Contains(stmt, "pg_advisory") || strings.Contains(stmt, "lock_timeout") {
				t.Fatalf("Statements must not contain lock statements: %q", stmt)
			}
		}
	})
	t.Run("Default_Statements_ContainNoAccountGrants", func(t *testing.T) {
		stmts, err := postgresschema.Statements("public")
		if err != nil {
			t.Fatalf("Statements: %v", err)
		}
		joined := strings.ToUpper(strings.Join(stmts, "\n"))
		for _, forbidden := range []string{"GRANT ", "CREATE ROLE", "CREATE USER", "ALTER ROLE"} {
			if strings.Contains(joined, forbidden) {
				t.Fatalf("Statements must not contain %q", forbidden)
			}
		}
	})
	t.Run("Default_Statements_StableAcrossCalls", func(t *testing.T) {
		first, err := postgresschema.Statements("public")
		if err != nil {
			t.Fatalf("Statements: %v", err)
		}
		second, err := postgresschema.Statements("public")
		if err != nil {
			t.Fatalf("Statements second call: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatal("Statements is not stable across calls")
		}
	})
}

func TestSQL(t *testing.T) {
	t.Run("InvalidSchema_SQL_ReturnsError", func(t *testing.T) {
		if _, err := postgresschema.SQL("bad-name"); err == nil {
			t.Fatal("SQL(invalid) = nil error, want error")
		}
	})
	t.Run("Default_SQL_LockFirstThenBaselineThenCommit", func(t *testing.T) {
		script, err := postgresschema.SQL("tenant")
		if err != nil {
			t.Fatalf("SQL: %v", err)
		}
		lockPos := strings.Index(script, "SELECT pg_advisory_xact_lock(hashtext('go_event:tenant'))")
		ddlPos := strings.Index(script, "CREATE SCHEMA IF NOT EXISTS tenant")
		commitPos := strings.LastIndex(script, "COMMIT;")
		if !strings.HasPrefix(script, "BEGIN;") {
			t.Fatalf("SQL must start with BEGIN;, got %q", firstLine(script))
		}
		if lockPos < 0 {
			t.Fatal("SQL missing advisory lock statement")
		}
		if ddlPos < 0 || lockPos > ddlPos {
			t.Fatal("SQL must acquire the lock before the first DDL statement")
		}
		if commitPos < 0 || commitPos < ddlPos {
			t.Fatal("SQL must end with COMMIT; after the baseline")
		}
		if !strings.Contains(script, "SET LOCAL lock_timeout") {
			t.Fatal("SQL missing SET LOCAL lock_timeout")
		}
	})
	t.Run("Default_SQL_StableAcrossCalls", func(t *testing.T) {
		first, err := postgresschema.SQL("public")
		if err != nil {
			t.Fatalf("SQL: %v", err)
		}
		second, err := postgresschema.SQL("public")
		if err != nil {
			t.Fatalf("SQL second call: %v", err)
		}
		if first != second {
			t.Fatal("SQL is not stable across calls")
		}
	})
}

func TestExpectedObjects(t *testing.T) {
	t.Run("InvalidSchema_ExpectedObjects_ReturnsNil", func(t *testing.T) {
		if objs := postgresschema.ExpectedObjects("bad-name"); objs != nil {
			t.Fatalf("ExpectedObjects(invalid) = %d objects, want nil", len(objs))
		}
	})
	t.Run("Default_ExpectedObjects_CoverBaselineAndVersionsInStableOrder", func(t *testing.T) {
		objs := postgresschema.ExpectedObjects("public")
		wantKinds := []event.ObjectKind{
			event.KindSchema,
			event.KindTable,
			event.KindConstraint,
			event.KindConstraint,
			event.KindIndex,
			event.KindTable,
			event.KindConstraint,
			event.KindConstraint,
			event.KindIndex,
			event.KindColumn,
			event.KindColumn,
			event.KindColumn,
		}
		if len(objs) != len(wantKinds) {
			t.Fatalf("ExpectedObjects = %d, want %d", len(objs), len(wantKinds))
		}
		for i, want := range wantKinds {
			if objs[i].Kind != want {
				t.Fatalf("object %d kind = %q, want %q", i, objs[i].Kind, want)
			}
		}
		if objs[0].Name != "public" {
			t.Fatalf("schema object = %q, want public", objs[0].Name)
		}
		if objs[1].Name != "public.event" || len(objs[1].Columns) != 6 {
			t.Fatalf("event table object = %+v", objs[1])
		}
		var positionSeq bool
		for _, col := range objs[1].Columns {
			if col.Name == "position" {
				positionSeq = col.Sequence
			}
		}
		if !positionSeq {
			t.Fatalf("event.position must be declared as sequence-backed: %+v", objs[1].Columns)
		}
		if objs[6].Name != "public.event_claim_pk" ||
			!reflect.DeepEqual(objs[6].KeyColumns, []string{"event_id", "consumer_type"}) {
			t.Fatalf("event_claim_pk object = %+v", objs[6])
		}
		if objs[7].ForeignKey == nil || objs[7].ForeignKey.RefTable != "event" {
			t.Fatalf("foreign key object = %+v", objs[7])
		}
		// v2 版本加列对象：限定名含列、归属表为 event_claim、单列可空定义。
		wantCols := []string{"public.event_claim.last_error", "public.event_claim.failed_at"}
		for i, name := range wantCols {
			obj := objs[9+i]
			if obj.Name != name || obj.Table != "event_claim" {
				t.Fatalf("version column object %d = %+v, want %s on event_claim", 9+i, obj, name)
			}
			if len(obj.Columns) != 1 || obj.Columns[0].NotNull {
				t.Fatalf("version column object %s must carry a single nullable column: %+v", name, obj.Columns)
			}
		}
		if objs[9].Columns[0].DataType != "text" ||
			objs[10].Columns[0].DataType != "timestamp with time zone" {
			t.Fatalf("version column types = %q / %q, want text / timestamp with time zone",
				objs[9].Columns[0].DataType, objs[10].Columns[0].DataType)
		}
		// v3 版本加列对象：event.deliver_at，单列可空定义（零值 = 立即）。
		obj := objs[11]
		if obj.Name != "public.event.deliver_at" || obj.Table != "event" {
			t.Fatalf("v3 column object = %+v, want public.event.deliver_at on event", obj)
		}
		if len(obj.Columns) != 1 || obj.Columns[0].NotNull ||
			obj.Columns[0].DataType != "timestamp with time zone" {
			t.Fatalf("v3 column object must carry a single nullable timestamptz: %+v", obj.Columns)
		}
	})
}

func TestRepairable(t *testing.T) {
	tests := []struct {
		name   string
		report event.VerifyReport
		want   bool
	}{
		{"EmptyReport_Repairable_ReturnsTrue", event.VerifyReport{}, true},
		{"MissingSchema_Repairable_ReturnsTrue", reportOf(event.KindSchema, event.ProblemMissing), true},
		{"MissingTable_Repairable_ReturnsTrue", reportOf(event.KindTable, event.ProblemMissing), true},
		{"MissingIndex_Repairable_ReturnsTrue", reportOf(event.KindIndex, event.ProblemMissing), true},
		{"MissingConstraint_Repairable_ReturnsTrue", reportOf(event.KindConstraint, event.ProblemMissing), true},
		{"MissingVersionColumn_Repairable_ReturnsTrue", reportOf(event.KindColumn, event.ProblemMissing), true},
		{"OutdatedTable_Repairable_ReturnsFalse", reportOf(event.KindTable, event.ProblemOutdated), false},
		{"OutdatedColumn_Repairable_ReturnsFalse", reportOf(event.KindColumn, event.ProblemOutdated), false},
		{"OutdatedConstraint_Repairable_ReturnsFalse", reportOf(event.KindConstraint, event.ProblemOutdated), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := postgresschema.Repairable(tt.report); got != tt.want {
				t.Fatalf("Repairable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiff(t *testing.T) {
	t.Run("EmptySnapshot_Diff_ReportsEveryBaselineObjectMissing", func(t *testing.T) {
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), postgresschema.Snapshot{})
		expected := postgresschema.ExpectedObjects("public")
		if len(report.Issues) != len(expected) {
			t.Fatalf("issues = %d, want %d", len(report.Issues), len(expected))
		}
		for i, issue := range report.Issues {
			if issue.Problem != event.ProblemMissing {
				t.Fatalf("issue %d problem = %q, want missing", i, issue.Problem)
			}
			if issue.Object == "" || issue.Kind == "" || issue.Action == "" {
				t.Fatalf("issue %d has empty fields: %+v", i, issue)
			}
			if issue.Kind != expected[i].Kind {
				t.Fatalf("issue %d kind = %q, want %q", i, issue.Kind, expected[i].Kind)
			}
		}
	})
	t.Run("FullSnapshot_Diff_ReportsNoIssues", func(t *testing.T) {
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), fullSnapshot())
		if !report.OK() {
			t.Fatalf("issues = %+v, want none", report.Issues)
		}
	})
	t.Run("WrongColumnType_Diff_ReportsOutdatedColumn", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event"]
		for i := range table.Columns {
			if table.Columns[i].Name == "payload" {
				table.Columns[i].DataType = "text"
			}
		}
		snap.Tables["event"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event.payload")
		if !ok {
			t.Fatalf("issues = %+v, want outdated for public.event.payload", report.Issues)
		}
		if issue.Kind != event.KindColumn || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issue = %+v, want column/outdated", issue)
		}
	})
	t.Run("MissingBaselineColumn_Diff_ReportsOutdatedNotRepairable", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event"]
		var kept []postgresschema.ColumnState
		for _, col := range table.Columns {
			if col.Name != "metadata" {
				kept = append(kept, col)
			}
		}
		table.Columns = kept
		snap.Tables["event"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event.metadata")
		if !ok {
			t.Fatalf("issues = %+v, want report for public.event.metadata", report.Issues)
		}
		// 基线列缺失属定义偏差（迁移不改既有表），报告 outdated 而非 missing。
		if issue.Kind != event.KindColumn || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issue = %+v, want column/outdated", issue)
		}
	})
	t.Run("MissingVersionColumn_Diff_ReportsMissingRepairable", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event_claim"]
		var kept []postgresschema.ColumnState
		for _, col := range table.Columns {
			if col.Name != "failed_at" {
				kept = append(kept, col)
			}
		}
		table.Columns = kept
		snap.Tables["event_claim"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_claim.failed_at")
		if !ok {
			t.Fatalf("issues = %+v, want report for public.event_claim.failed_at", report.Issues)
		}
		if issue.Kind != event.KindColumn || issue.Problem != event.ProblemMissing {
			t.Fatalf("issue = %+v, want column/missing", issue)
		}
		if !postgresschema.Repairable(report) {
			t.Fatalf("missing version column must be repairable, issues = %+v", report.Issues)
		}
	})
	t.Run("WrongVersionColumnType_Diff_ReportsOutdatedColumn", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event_claim"]
		for i := range table.Columns {
			if table.Columns[i].Name == "last_error" {
				table.Columns[i].DataType = "integer"
			}
		}
		snap.Tables["event_claim"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_claim.last_error")
		if !ok {
			t.Fatalf("issues = %+v, want report for public.event_claim.last_error", report.Issues)
		}
		if issue.Kind != event.KindColumn || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issue = %+v, want column/outdated", issue)
		}
		if postgresschema.Repairable(report) {
			t.Fatal("outdated column definition must not be repairable")
		}
	})
	t.Run("MissingTable_Diff_ReportsVersionColumnsMissing", func(t *testing.T) {
		// 表缺失时列对象按缺失报告（每对象恰一问题，随表补齐收敛）。
		snap := fullSnapshot()
		delete(snap.Tables, "event_claim")
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_claim.last_error")
		if !ok || issue.Problem != event.ProblemMissing {
			t.Fatalf("issues = %+v, want missing public.event_claim.last_error", report.Issues)
		}
	})
	t.Run("WrongPrimaryKeyColumns_Diff_ReportsOutdatedConstraint", func(t *testing.T) {
		snap := fullSnapshot()
		pk := snap.Constraints["event_pk"]
		pk.Columns = []string{"type"}
		snap.Constraints["event_pk"] = pk
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_pk")
		if !ok || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated public.event_pk", report.Issues)
		}
	})
	t.Run("WrongIndexColumns_Diff_ReportsOutdatedIndex", func(t *testing.T) {
		snap := fullSnapshot()
		idx := snap.Indexes["event_type_position_idx"]
		idx.Columns = []string{"position"}
		snap.Indexes["event_type_position_idx"] = idx
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_type_position_idx")
		if !ok || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated event_type_position_idx", report.Issues)
		}
	})
	t.Run("WrongForeignTarget_Diff_ReportsOutdatedConstraint", func(t *testing.T) {
		snap := fullSnapshot()
		fk := snap.Constraints["event_claim_event_id_fkey"]
		fk.RefTable = "other"
		snap.Constraints["event_claim_event_id_fkey"] = fk
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_claim_event_id_fkey")
		if !ok || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated foreign key", report.Issues)
		}
	})
	t.Run("MissingSequenceDefault_Diff_ReportsOutdatedPosition", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event"]
		for i := range table.Columns {
			if table.Columns[i].Name == "position" {
				table.Columns[i].HasSequenceDefault = false
			}
		}
		snap.Tables["event"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event.position")
		if !ok || issue.Kind != event.KindColumn || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated column public.event.position", report.Issues)
		}
	})
	t.Run("IndexOnOtherTable_Diff_ReportsOutdatedIndex", func(t *testing.T) {
		snap := fullSnapshot()
		idx := snap.Indexes["event_type_position_idx"]
		idx.Table = "event_claim"
		snap.Indexes["event_type_position_idx"] = idx
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_type_position_idx")
		if !ok || issue.Kind != event.KindIndex || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated index on other table", report.Issues)
		}
	})
	t.Run("RelationKindView_Diff_ReportsOutdatedTable", func(t *testing.T) {
		snap := fullSnapshot()
		table := snap.Tables["event_claim"]
		table.RelKind = "v"
		snap.Tables["event_claim"] = table
		report := postgresschema.Diff(postgresschema.ExpectedObjects("public"), snap)
		issue, ok := findIssue(report, "public.event_claim")
		if !ok || issue.Kind != event.KindTable || issue.Problem != event.ProblemOutdated {
			t.Fatalf("issues = %+v, want outdated table public.event_claim", report.Issues)
		}
	})
}

func fullSnapshot() postgresschema.Snapshot {
	eventCols := make([]postgresschema.ColumnState, 0, 7)
	for _, col := range append(postgresschema.EventColumns(), postgresschema.EventVersionColumns()...) {
		eventCols = append(eventCols, postgresschema.ColumnState{
			Name: col.Name, DataType: col.DataType, NotNull: col.NotNull,
			HasSequenceDefault: col.Sequence,
		})
	}
	claimCols := make([]postgresschema.ColumnState, 0, 9)
	for _, col := range append(postgresschema.EventClaimColumns(), postgresschema.EventClaimVersionColumns()...) {
		claimCols = append(claimCols, postgresschema.ColumnState{
			Name: col.Name, DataType: col.DataType, NotNull: col.NotNull,
			HasSequenceDefault: col.Sequence,
		})
	}
	return postgresschema.Snapshot{
		SchemaExists: true,
		Tables: map[string]postgresschema.TableState{
			"event":       {RelKind: "r", Columns: eventCols},
			"event_claim": {RelKind: "r", Columns: claimCols},
		},
		Constraints: map[string]postgresschema.ConstraintState{
			"event_pk": {
				Name: "event_pk", Type: "p", Table: "event", Columns: []string{"id"},
			},
			"event_type_id_uniq": {
				Name: "event_type_id_uniq", Type: "u", Table: "event", Columns: []string{"type", "id"},
			},
			"event_claim_pk": {
				Name: "event_claim_pk", Type: "p", Table: "event_claim",
				Columns: []string{"event_id", "consumer_type"},
			},
			"event_claim_event_id_fkey": {
				Name: "event_claim_event_id_fkey", Type: "f", Table: "event_claim",
				Columns: []string{"event_id"}, RefTable: "event", RefColumns: []string{"id"},
			},
		},
		Indexes: map[string]postgresschema.IndexState{
			"event_type_position_idx": {
				Name: "event_type_position_idx", Table: "event", Columns: []string{"type", "position"},
			},
			"event_claim_eligible_idx": {
				Name: "event_claim_eligible_idx", Table: "event_claim",
				Columns: []string{"status", "next_eligible_at"},
			},
		},
	}
}

func reportOf(kind event.ObjectKind, problem event.VerifyProblem) event.VerifyReport {
	return event.VerifyReport{Issues: []event.VerifyIssue{{
		Object: "public.event", Kind: kind, Problem: problem, Action: "fix",
	}}}
}

func findIssue(report event.VerifyReport, object string) (event.VerifyIssue, bool) {
	for _, issue := range report.Issues {
		if issue.Object == object {
			return issue, true
		}
	}
	return event.VerifyIssue{}, false
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
