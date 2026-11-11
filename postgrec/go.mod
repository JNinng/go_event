module github.com/jninng/go_event/postgrec

go 1.26.8

require (
	github.com/jackc/pgx/v5 v5.7.6
	github.com/jninng/go_event v0.0.0
	github.com/jninng/go_event/eventtest v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)

// 本地开发替换（ADR 0001）：发布为嵌套 tag 后，对下游消费方此指令
// 自动忽略；仓库内配合 go.work 使用。
replace github.com/jninng/go_event => ../

replace github.com/jninng/go_event/eventtest => ../eventtest
