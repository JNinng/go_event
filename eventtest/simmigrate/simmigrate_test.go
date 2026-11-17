package simmigrate_test

import (
	"testing"

	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/eventtest/simmigrate"
)

// 模拟 backend 始终运行：全部语句边界故障点与 Lock=none 变体由它覆盖
// （真实 Postgres 由 TEST_POSTGRES_DSN 门控，仅覆盖部分语句边界故障点）。
func TestSimMigrateContractSuite(t *testing.T) {
	eventtest.RunMigrationSuite(t, simmigrate.New())
}
