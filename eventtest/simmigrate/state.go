package simmigrate

import (
	"fmt"
	"slices"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// state 是内存 catalog 的持久结构：schema、表（含列）、约束与索引。
type state struct {
	schemaExists bool
	tables       map[string]tableState
	constraints  map[string]constraintState
	indexes      map[string]indexState
}

type tableState struct {
	columns []postgresschema.Column
}

type constraintState struct {
	table      string
	typ        string // p / u / f
	columns    []string
	refTable   string
	refColumns []string
}

type indexState struct {
	table   string
	unique  bool
	columns []string
}

func newState() *state {
	return &state{
		tables:      map[string]tableState{},
		constraints: map[string]constraintState{},
		indexes:     map[string]indexState{},
	}
}

func (s *state) clone() *state {
	out := newState()
	out.schemaExists = s.schemaExists
	for name, tbl := range s.tables {
		out.tables[name] = tableState{columns: slices.Clone(tbl.columns)}
	}
	for name, c := range s.constraints {
		c.columns = slices.Clone(c.columns)
		c.refColumns = slices.Clone(c.refColumns)
		out.constraints[name] = c
	}
	for name, idx := range s.indexes {
		idx.columns = slices.Clone(idx.columns)
		out.indexes[name] = idx
	}
	return out
}

// snapshot 把内存状态转换为 postgresschema 的目录快照，供 Diff/Repairable
// 判定（与真实后端同一套语义）。
func (s *state) snapshot() postgresschema.Snapshot {
	snap := postgresschema.Snapshot{
		SchemaExists: s.schemaExists,
		Tables:       make(map[string]postgresschema.TableState, len(s.tables)),
		Constraints:  make(map[string]postgresschema.ConstraintState, len(s.constraints)),
		Indexes:      make(map[string]postgresschema.IndexState, len(s.indexes)),
	}
	for name, tbl := range s.tables {
		ts := postgresschema.TableState{RelKind: "r"}
		for _, col := range tbl.columns {
			ts.Columns = append(ts.Columns, postgresschema.ColumnState{
				Name:               col.Name,
				DataType:           col.DataType,
				NotNull:            col.NotNull,
				HasSequenceDefault: col.Sequence,
			})
		}
		snap.Tables[name] = ts
	}
	for name, c := range s.constraints {
		snap.Constraints[name] = postgresschema.ConstraintState{
			Name:       name,
			Type:       c.typ,
			Table:      c.table,
			Columns:    slices.Clone(c.columns),
			RefTable:   c.refTable,
			RefColumns: slices.Clone(c.refColumns),
		}
	}
	for name, idx := range s.indexes {
		snap.Indexes[name] = postgresschema.IndexState{
			Name:    name,
			Table:   idx.table,
			Unique:  idx.unique,
			Columns: slices.Clone(idx.columns),
		}
	}
	return snap
}

// applyBaseline 直接应用基线期望对象构造存量场景（limit<=0 表示全部）。
func (s *state) applyBaseline(limit int) error {
	objs := postgresschema.ExpectedObjects(schema)
	if limit > 0 && limit < len(objs) {
		objs = objs[:limit]
	}
	for _, obj := range objs {
		if _, err := s.applyObject(obj); err != nil {
			return err
		}
	}
	return nil
}

// buildConflict 构造定义冲突场景：schema 与 event 表已存在，但 payload 为
// text（基线 bytea）；主键与基线一致，其余对象缺失。
func (s *state) buildConflict() {
	s.schemaExists = true
	columns := postgresschema.EventColumns()
	for i := range columns {
		if columns[i].Name == "payload" {
			columns[i].DataType = "text"
		}
	}
	s.tables[postgresschema.TableEvent] = tableState{columns: columns}
	s.constraints["event_pk"] = constraintState{
		table:   postgresschema.TableEvent,
		typ:     "p",
		columns: []string{"id"},
	}
}

// applyObject 直接创建/覆盖单个对象，不做探测。调用方负责探测与冲突判定：
// 模拟执行器在接管校验（全局 Diff）后按缺失集合在 staging 上应用。
func (s *state) applyObject(obj postgresschema.Object) (bool, error) {
	switch obj.Kind {
	case event.KindSchema:
		s.schemaExists = true
		return true, nil
	case event.KindTable:
		s.tables[obj.Table] = tableState{columns: slices.Clone(obj.Columns)}
		return true, nil
	case event.KindColumn:
		// 版本加列：对既有表追加列（ADD COLUMN IF NOT EXISTS 的模拟）。
		tbl, ok := s.tables[obj.Table]
		if !ok {
			return false, fmt.Errorf("simmigrate: table %q missing for column %s",
				obj.Table, obj.Name)
		}
		tbl.columns = append(slices.Clone(tbl.columns), obj.Columns...)
		s.tables[obj.Table] = tbl
		return true, nil
	case event.KindConstraint:
		if _, ok := s.tables[obj.Table]; !ok {
			return false, fmt.Errorf("simmigrate: table %q missing for constraint %s",
				obj.Table, obj.Name)
		}
		s.constraints[unqualified(obj.Name)] = constraintFromObject(obj)
		return true, nil
	case event.KindIndex:
		if _, ok := s.tables[obj.Table]; !ok {
			return false, fmt.Errorf("simmigrate: table %q missing for index %s",
				obj.Table, obj.Name)
		}
		s.indexes[unqualified(obj.Name)] = indexFromObject(obj)
		return true, nil
	default:
		return false, fmt.Errorf("simmigrate: unsupported object kind %q", obj.Kind)
	}
}

func constraintFromObject(obj postgresschema.Object) constraintState {
	cs := constraintState{
		table:   obj.Table,
		columns: slices.Clone(obj.KeyColumns),
	}
	switch {
	case obj.ForeignKey != nil:
		cs.typ = "f"
		cs.refTable = obj.ForeignKey.RefTable
		cs.refColumns = slices.Clone(obj.ForeignKey.RefColumns)
	case obj.Unique:
		cs.typ = "u"
	default:
		cs.typ = "p"
	}
	return cs
}

func indexFromObject(obj postgresschema.Object) indexState {
	return indexState{
		table:   obj.Table,
		unique:  obj.Unique,
		columns: slices.Clone(obj.KeyColumns),
	}
}
