package pgxc

import (
	"time"

	"github.com/jninng/go_event/postgresschema"
)

// SetStepHookForTest 替换 fast 执行器的步骤钩子（仅测试编译单元可见），
// 返回恢复函数。用于在指定步骤注入失败以验证整体回滚。
func SetStepHookForTest(fn func(postgresschema.Step) error) func() {
	old := testStepHook
	testStepHook = fn
	return func() { testStepHook = old }
}

// SetLockWaitTimeoutForTest 缩短迁移锁等待上限（仅测试编译单元可见），
// 返回恢复函数。
func SetLockWaitTimeoutForTest(d time.Duration) func() {
	old := lockWaitTimeout
	lockWaitTimeout = d
	return func() { lockWaitTimeout = old }
}
