package simmigrate_test

import (
	"testing"

	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/eventtest/simmigrate"
)

// 模拟 backend 始终运行：fast / step 两条路径、故障注入与 Lock=none 变体
// 都由它覆盖（真实 Postgres 由 TEST_POSTGRES_DSN 门控，仅跑 fast 默认路径）。
func TestSimMigrateContractSuite(t *testing.T) {
	eventtest.RunMigrationSuite(t, simmigrate.New())
}
