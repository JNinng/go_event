package postgresschema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// fakeTx 是 MigrationTx 的内存替身：以固定快照应答 ReadSnapshot 的目录
// 查询（按查询常量精确匹配），记录执行过的语句与提交/回滚，并支持注入
// try-lock 结果与故障。不触碰真实数据库。
type fakeTx struct {
	snap Snapshot

	// tryLocks 依次应答 try-lock 查询；耗尽后重复最后一个值。
	tryLocks     []bool
	tryLockCalls int
	// onTryLock 在每次 try-lock 应答后调用（ctx 取消注入用）。
	onTryLock func()

	queryErr  error // ReadSnapshot 目录查询失败
	commitErr error
	// failOnExec 使第 N 次 Exec 失败（1 起）：1 是 lock_timeout 设置，
	// 其后为计划内语句。
	failOnExec int
	failErr    error

	execCalls  int
	execs      []string
	committed  bool
	rolledBack bool
}

var (
	_ MigrationTx        = (*fakeTx)(nil)
	_ MigrationConnector = (*fakeConnector)(nil)
)

func (tx *fakeTx) ExecContext(_ context.Context, query string, _ ...any) error {
	tx.execCalls++
	tx.execs = append(tx.execs, query)
	if tx.failOnExec == tx.execCalls {
		return tx.failErr
	}
	return nil
}

func (tx *fakeTx) QueryContext(_ context.Context, query string, _ ...any) (Rows, error) {
	if tx.queryErr != nil {
		return nil, tx.queryErr
	}
	return tx.snapshotRows(query), nil
}

func (tx *fakeTx) QueryRowContext(_ context.Context, _ string, _ ...any) MigrationRow {
	tx.tryLockCalls++
	ok := tx.tryLocks[len(tx.tryLocks)-1]
	if tx.tryLockCalls <= len(tx.tryLocks) {
		ok = tx.tryLocks[tx.tryLockCalls-1]
	}
	if tx.onTryLock != nil {
		tx.onTryLock()
	}
	return fakeRow{ok: ok}
}

func (tx *fakeTx) Commit(context.Context) error {
	if tx.commitErr != nil {
		return tx.commitErr
	}
	tx.committed = true
	return nil
}

func (tx *fakeTx) Rollback(context.Context) error {
	tx.rolledBack = true
	return nil
}

// snapshotRows 把快照转换为 ReadSnapshot 各目录查询的应答行。
func (tx *fakeTx) snapshotRows(query string) *fakeRows {
	switch query {
	case schemaExistsQuery:
		return &fakeRows{rows: [][]any{{tx.snap.SchemaExists}}}
	case tablesQuery:
		var rows [][]any
		for _, name := range sortedKeys(tx.snap.Tables) {
			rows = append(rows, []any{name, tx.snap.Tables[name].RelKind})
		}
		return &fakeRows{rows: rows}
	case columnsQuery:
		var rows [][]any
		for _, name := range sortedKeys(tx.snap.Tables) {
			for _, col := range tx.snap.Tables[name].Columns {
				def := ""
				if col.HasSequenceDefault {
					def = "nextval('seq')"
				}
				rows = append(rows, []any{name, col.Name, col.DataType, col.NotNull, def})
			}
		}
		return &fakeRows{rows: rows}
	case constraintsQuery:
		var rows [][]any
		for _, name := range sortedKeys(tx.snap.Constraints) {
			c := tx.snap.Constraints[name]
			rows = append(rows, []any{
				c.Name, c.Type, c.Table,
				strings.Join(c.Columns, ","), c.RefTable, strings.Join(c.RefColumns, ","),
			})
		}
		return &fakeRows{rows: rows}
	case indexesQuery:
		var rows [][]any
		for _, name := range sortedKeys(tx.snap.Indexes) {
			idx := tx.snap.Indexes[name]
			rows = append(rows, []any{idx.Name, idx.Table, idx.Unique, strings.Join(idx.Columns, ",")})
		}
		return &fakeRows{rows: rows}
	}
	return &fakeRows{}
}

// fakeRow 是 try-lock 单行应答。
type fakeRow struct{ ok bool }

func (r fakeRow) Scan(dest ...any) error {
	for _, d := range dest {
		if p, ok := d.(*bool); ok {
			*p = r.ok
			return nil
		}
	}
	return fmt.Errorf("fakeRow: no *bool dest")
}

// fakeRows 是固定行集的只读应答（bool / string 列）。
type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool { return r.i < len(r.rows) }

func (r *fakeRows) Scan(dest ...any) error {
	if r.i >= len(r.rows) {
		return fmt.Errorf("fakeRows: no more rows")
	}
	row := r.rows[r.i]
	r.i++
	for j, d := range dest {
		switch p := d.(type) {
		case *bool:
			*p = row[j].(bool)
		case *string:
			*p = row[j].(string)
		default:
			return fmt.Errorf("fakeRows: unsupported dest %T", d)
		}
	}
	return nil
}

func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

// fakeConnector 发放固定 fakeTx，可注入 begin 失败。
type fakeConnector struct {
	tx         *fakeTx
	beginErr   error
	beginCalls int
}

func (c *fakeConnector) BeginMigration(context.Context) (MigrationTx, error) {
	c.beginCalls++
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return c.tx, nil
}

// newFakeBackend 构造 connector 与 tx 替身；tryLocks 缺省为 [true]（一次取锁成功）。
func newFakeBackend(snap Snapshot, tryLocks ...bool) (*fakeConnector, *fakeTx) {
	if len(tryLocks) == 0 {
		tryLocks = []bool{true}
	}
	tx := &fakeTx{snap: snap, tryLocks: tryLocks}
	return &fakeConnector{tx: tx}, tx
}

// planExecs 返回已执行的计划语句（去掉首条 lock_timeout 设置）。
func planExecs(tx *fakeTx) []string { return tx.execs[1:] }

// consistentSnapshot 构造与期望对象完全一致的快照（全量接管场景）。
func consistentSnapshot() Snapshot {
	toColStates := func(cols []Column) []ColumnState {
		states := make([]ColumnState, 0, len(cols))
		for _, c := range cols {
			states = append(states, ColumnState{
				Name: c.Name, DataType: c.DataType, NotNull: c.NotNull,
				HasSequenceDefault: c.Sequence,
			})
		}
		return states
	}
	return Snapshot{
		SchemaExists: true,
		Tables: map[string]TableState{
			TableEvent:      {RelKind: "r", Columns: toColStates(EventColumns())},
			TableEventClaim: {RelKind: "r", Columns: toColStates(append(EventClaimColumns(), EventClaimVersionColumns()...))},
		},
		Constraints: map[string]ConstraintState{
			"event_pk":           {Name: "event_pk", Type: "p", Table: TableEvent, Columns: []string{"id"}},
			"event_type_id_uniq": {Name: "event_type_id_uniq", Type: "u", Table: TableEvent, Columns: []string{"type", "id"}},
			"event_claim_pk":     {Name: "event_claim_pk", Type: "p", Table: TableEventClaim, Columns: []string{"event_id", "consumer_type"}},
			"event_claim_event_id_fkey": {
				Name: "event_claim_event_id_fkey", Type: "f", Table: TableEventClaim,
				Columns: []string{"event_id"}, RefTable: TableEvent, RefColumns: []string{"id"},
			},
		},
		Indexes: map[string]IndexState{
			"event_type_position_idx":  {Name: "event_type_position_idx", Table: TableEvent, Columns: []string{"type", "position"}},
			"event_claim_eligible_idx": {Name: "event_claim_eligible_idx", Table: TableEventClaim, Columns: []string{"status", "next_eligible_at"}},
		},
	}
}

func sortedKeys[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestMigrate(t *testing.T) {
	const schema = "unit"
	plan, err := PlanStatements(schema)
	if err != nil {
		t.Fatalf("PlanStatements: %v", err)
	}

	t.Run("InvalidSchema_Migrate_RejectsBeforeBegin", func(t *testing.T) {
		conn, _ := newFakeBackend(Snapshot{})
		err := Migrate(context.Background(), conn, "bad-name")
		if !errors.Is(err, event.ErrInvalidSchema) {
			t.Fatalf("migrate = %v, want ErrInvalidSchema", err)
		}
		if conn.beginCalls != 0 {
			t.Fatalf("begin calls = %d, want 0 (no tx before validation)", conn.beginCalls)
		}
	})

	t.Run("BeginMigrationFails_Migrate_ReturnsBeginError", func(t *testing.T) {
		conn, _ := newFakeBackend(Snapshot{})
		conn.beginErr = errors.New("boom begin")
		err := Migrate(context.Background(), conn, schema)
		if err == nil || !strings.Contains(err.Error(), "begin tx") {
			t.Fatalf("migrate = %v, want begin tx error", err)
		}
	})

	t.Run("SnapshotQueryFails_Migrate_RollsBackWithoutDDL", func(t *testing.T) {
		conn, tx := newFakeBackend(Snapshot{})
		tx.queryErr = errors.New("boom query")
		err := Migrate(context.Background(), conn, schema)
		if err == nil || !strings.Contains(err.Error(), "read snapshot") {
			t.Fatalf("migrate = %v, want read snapshot error", err)
		}
		if !tx.rolledBack {
			t.Fatal("tx must be rolled back on snapshot failure")
		}
		for _, q := range tx.execs {
			if strings.Contains(q, "CREATE") {
				t.Fatalf("DDL executed despite snapshot failure: %q", q)
			}
		}
	})

	t.Run("ConflictingRelation_Migrate_ReturnsVerifyErrorWithoutDDL", func(t *testing.T) {
		snap := consistentSnapshot()
		table := snap.Tables[TableEvent]
		table.RelKind = "v"
		snap.Tables[TableEvent] = table
		conn, tx := newFakeBackend(snap)

		err := Migrate(context.Background(), conn, schema)
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("migrate = %v, want *VerifyError", err)
		}
		if len(verifyErr.Report.Issues) == 0 {
			t.Fatal("VerifyError carries no issues")
		}
		if len(planExecs(tx)) != 0 {
			t.Fatalf("plan statements executed despite conflict: %d", len(planExecs(tx)))
		}
		if !tx.rolledBack {
			t.Fatal("tx must be rolled back on conflict")
		}
	})

	t.Run("EmptyDatabase_Migrate_ExecutesPlanInOrderAndCommits", func(t *testing.T) {
		conn, tx := newFakeBackend(Snapshot{})
		if err := Migrate(context.Background(), conn, schema, nil); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if got := planExecs(tx); !reflect.DeepEqual(got, plan) {
			t.Fatalf("executed statements = %d, want full plan (%d) in order", len(got), len(plan))
		}
		if !tx.committed {
			t.Fatal("tx must be committed")
		}
	})

	t.Run("ConsistentObjects_Migrate_ReplaysFullPlanAndCommits", func(t *testing.T) {
		conn, tx := newFakeBackend(consistentSnapshot())
		if err := Migrate(context.Background(), conn, schema); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if got := planExecs(tx); !reflect.DeepEqual(got, plan) {
			t.Fatalf("replayed statements = %d, want full plan (%d) in order", len(got), len(plan))
		}
		if !tx.committed {
			t.Fatal("tx must be committed")
		}
	})

	t.Run("LockFreedOnRetry_Migrate_AcquiresLockAndCommits", func(t *testing.T) {
		conn, tx := newFakeBackend(Snapshot{}, false, true)
		if err := Migrate(context.Background(), conn, schema); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if tx.tryLockCalls != 2 {
			t.Fatalf("try-lock calls = %d, want 2", tx.tryLockCalls)
		}
		if !tx.committed {
			t.Fatal("tx must be committed")
		}
	})

	t.Run("LockNeverAcquired_Migrate_ReturnsLockTimeoutError", func(t *testing.T) {
		conn, tx := newFakeBackend(Snapshot{}, false)
		err := Migrate(context.Background(), conn, schema, WithMigrationLockWait(20*time.Millisecond))
		if !errors.Is(err, event.ErrMigrationLockTimeout) {
			t.Fatalf("migrate = %v, want ErrMigrationLockTimeout", err)
		}
		if !tx.rolledBack {
			t.Fatal("tx must be rolled back on lock timeout")
		}
	})

	t.Run("ContextCanceledWhileWaitingForLock_Migrate_ReturnsContextError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		conn, tx := newFakeBackend(Snapshot{}, false)
		tx.onTryLock = cancel
		err := Migrate(ctx, conn, schema)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("migrate = %v, want context.Canceled", err)
		}
	})

	t.Run("HookRejectsSecondStatement_Migrate_AbortsBeforeExecAndRollsBack", func(t *testing.T) {
		injected := errors.New("hook stop")
		hookCalls := 0
		conn, tx := newFakeBackend(Snapshot{})
		err := Migrate(context.Background(), conn, schema, WithMigrationHook(func(PlannedStatement) error {
			hookCalls++
			if hookCalls == 2 {
				return injected
			}
			return nil
		}))
		if !errors.Is(err, injected) {
			t.Fatalf("migrate = %v, want injected hook failure", err)
		}
		if got := planExecs(tx); !reflect.DeepEqual(got, plan[:1]) {
			t.Fatalf("executed statements = %d, want only the first", len(got))
		}
		if !tx.rolledBack {
			t.Fatal("tx must be rolled back on hook failure")
		}
	})

	t.Run("StatementExecFails_Migrate_ReturnsRawErrorAndRollsBack", func(t *testing.T) {
		boom := errors.New("boom exec")
		conn, tx := newFakeBackend(Snapshot{})
		tx.failOnExec, tx.failErr = 4, boom // 第 3 条计划语句
		err := Migrate(context.Background(), conn, schema)
		if !errors.Is(err, boom) {
			t.Fatalf("migrate = %v, want raw exec failure", err)
		}
		if got := planExecs(tx); !reflect.DeepEqual(got, plan[:3]) {
			t.Fatalf("attempted statements = %d, want first three", len(got))
		}
		if !tx.rolledBack {
			t.Fatal("tx must be rolled back on exec failure")
		}
	})

	t.Run("StatementExecFails_Migrate_ClassifierReceivesPairedStatement", func(t *testing.T) {
		boom := errors.New("boom exec")
		classified := errors.New("classified")
		var gotStmt PlannedStatement
		var gotErr error
		conn, tx := newFakeBackend(Snapshot{})
		tx.failOnExec, tx.failErr = 4, boom
		err := Migrate(context.Background(), conn, schema, WithMigrationClassify(func(ps PlannedStatement, err error) error {
			gotStmt, gotErr = ps, err
			return classified
		}))
		if !errors.Is(err, classified) {
			t.Fatalf("migrate = %v, want classified error", err)
		}
		if gotStmt.SQL != plan[2] {
			t.Fatalf("classifier got statement %q, want %q", gotStmt.SQL, plan[2])
		}
		if !errors.Is(gotErr, boom) {
			t.Fatalf("classifier got err %v, want raw exec failure", gotErr)
		}
	})

	t.Run("LockSetupFails_Migrate_ClassifierReceivesZeroValueStatement", func(t *testing.T) {
		boom := errors.New("boom lock setup")
		classified := errors.New("classified")
		var gotStmt PlannedStatement
		conn, tx := newFakeBackend(Snapshot{})
		tx.failOnExec, tx.failErr = 1, boom // lock_timeout 设置
		err := Migrate(context.Background(), conn, schema, WithMigrationClassify(func(ps PlannedStatement, err error) error {
			gotStmt = ps
			return classified
		}))
		if !errors.Is(err, classified) {
			t.Fatalf("migrate = %v, want classified error", err)
		}
		if gotStmt.SQL != "" || gotStmt.Object.Name != "" || gotStmt.Object.Kind != "" {
			t.Fatalf("classifier got %+v, want zero-value statement for lock setup", gotStmt)
		}
	})

	t.Run("CommitFails_Migrate_ReturnsCommitError", func(t *testing.T) {
		conn, tx := newFakeBackend(Snapshot{})
		tx.commitErr = errors.New("boom commit")
		err := Migrate(context.Background(), conn, schema)
		if err == nil || !strings.Contains(err.Error(), "commit") {
			t.Fatalf("migrate = %v, want commit error", err)
		}
	})
}
