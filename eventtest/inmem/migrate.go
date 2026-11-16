package inmem

import (
	"context"

	"github.com/jninng/go_event"
)

// Capabilities 返回进程内 Driver 的迁移能力声明：进程内 Driver 无持久
// 结构，迁移为 no-op（Create / Verify / None 均不产生 DDL），因此不做
// 任何能力声明（零值：无 DDL 原子性、无锁、无探测）。
func Capabilities() event.MigrationCapabilities {
	return event.MigrationCapabilities{}
}

// Migrate 是 no-op：进程内 Driver 无持久结构，不产生任何 DDL，恒返回 nil。
func Migrate(context.Context) error {
	return nil
}

// Verify 是 no-op：进程内 Driver 无持久结构，恒报告 OK 且无错误。
func Verify(context.Context) (event.VerifyReport, error) {
	return event.VerifyReport{}, nil
}
