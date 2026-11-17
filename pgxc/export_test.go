package pgxc

import (
	"time"

	"github.com/jninng/go_event/postgresschema"
)

// SetStatementHookForTest 替换执行器的语句钩子（仅测试编译单元可见），
// 返回恢复函数。用于在指定语句边界注入失败以验证整体回滚。
func SetStatementHookForTest(fn func(postgresschema.PlannedStatement) error) func() {
	old := testStatementHook
	testStatementHook = fn
	return func() { testStatementHook = old }
}

// SetLockWaitTimeoutForTest 缩短迁移锁等待上限（仅测试编译单元可见），
// 返回恢复函数。
func SetLockWaitTimeoutForTest(d time.Duration) func() {
	old := lockWaitTimeout
	lockWaitTimeout = d
	return func() { lockWaitTimeout = old }
}

// SetAdminLockWaitForTest 缩短管理写互斥锁等待上限（仅测试编译单元可见），
// 返回恢复函数。
func SetAdminLockWaitForTest(d time.Duration) func() {
	old := adminLockWait
	adminLockWait = d
	return func() { adminLockWait = old }
}
