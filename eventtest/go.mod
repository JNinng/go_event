module github.com/jninng/go_event/eventtest

go 1.26.8

require github.com/jninng/go_event v0.0.0

// 本地开发替换（ADR 0001）：发布为嵌套 tag 后，对下游消费方此指令
// 自动忽略；仓库内配合 go.work 使用。
replace github.com/jninng/go_event => ../
