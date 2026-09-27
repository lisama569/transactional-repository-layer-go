# transactional-repository-layer-go

围绕**清单（Manifest）**与**变更记录（ChangeRecord）**的本地数据访问层，
为上层业务提供事务组合、游标分页、条件更新与可解释的错误分类。
纯标准库实现，不依赖任何外部服务。

## 结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 领域类型：`Manifest`、`ChangeRecord`、`Filter`、`PageRequest`/`Page` |
| `errors.go` | 错误分类：`ErrorKind`、哨兵错误、`KindOf` 判定 |
| `store.go` | 存储引擎：内存数据 + JSON 快照持久化、原子提交、稳定分页游标 |
| `tx.go` | 事务：`Begin`/`Get`/`Create`/`Update`/`Delete`/`Commit`/`Rollback` |
| `repository.go` | 业务入口：`WithTx`、分页查询、`UpdateWithRetry` |

## 事务边界

- `Repository.WithTx(ctx, fn)` 是一次事务的完整边界：
  - `fn` 返回 `nil` 且提交成功 → 全部写入**一起可见**；
  - `fn` 返回错误、`ctx` 在提交前被取消、或提交（校验/落盘）失败 →
    **全部回滚，不留半成品**，调用方不会误以为操作已生效。
- 事务内可读到自己未提交的写入（read-your-writes）。
- 提交采用"工作副本"模式：所有变更先在副本上边校验边应用，
  全部通过并落盘成功后才原子换入内存状态；任一步失败内存保持原样。
- `Commit`/`Rollback` 之后事务即关闭，继续使用会返回 `tx_closed`。

## 分页与游标规则

- `ListManifests`：首页请求（游标为空）时固定一份**一致性快照**，
  后续翻页都在该快照上进行——翻页期间数据被新增或更新，
  结果不会重复也不会漏掉首页时已满足条件的数据。游标到达尾页后会话即失效。
- `ListChanges`：变更记录只增不改，使用全局单调 `Seq` 做 keyset 游标。
- `Limit` 必须在 `(0, 1000]`，否则报 `invalid_argument`。
- 游标为 base64(JSON) 且带类型标记：乱码、伪造、跨类型（清单/变更记录混用）、
  会话过期都报 `invalid_cursor`，与参数错误可单独识别。

## 条件更新与重试

- 所有更新/删除都携带 `expectVersion`（乐观并发控制），
  版本不匹配报 `conflict`，避免并发写入丢失更新。
- `Repository.UpdateWithRetry` 实现"读取-修改-写回"循环，
  仅在 `conflict` 时重试（其余错误直接返回），重试次数由 `maxRetries` 控制。
- **幂等键**：每次写入可带 `idempotencyKey`，已应用过的键在提交期去重为 no-op。
  同一逻辑操作无论重试多少次，都只产生一条变更记录，不重复产生副作用。

## 错误分类

| Kind | 含义 | 可重试？ |
| --- | --- | --- |
| `invalid_argument` | 参数错误（缺 ID、limit 越界等） | 否 |
| `invalid_cursor` | 游标非法 / 过期 / 类型不符 | 否（重新从首页查） |
| `not_found` | 目标不存在 | 否 |
| `conflict` | 版本冲突 / 并发更新 | 是（读-改-写重试） |
| `canceled` | 调用被取消 / 超时 | 视调用方语义 |
| `tx_closed` | 事务已结束仍被使用 | 否 |
| `internal` | 内部错误（含落盘失败） | 谨慎 |

判定方式：`KindOf(err)` 或 `errors.Is(err, txrepo.ErrConflict)` 等哨兵错误。

## 持久化

`txrepo.Open(path)` 打开存储：`path` 为空为纯内存；非空时在每次提交后
把全量状态以 JSON 快照写入（先写临时文件再 rename，保证不写坏），
重启后自动恢复数据与幂等键。

## 本地验证

```bash
go vet ./...
go test -race -count=1 -v ./...
```

测试全部在本地内存/临时目录中完成，覆盖：事务回滚
（`TestWithTxRollbackOnError`、`TestTxCommitConflictLeavesNothing`）、
并发更新不丢失（`TestConcurrentUpdatesNoLostUpdate`）、
幂等重放（`TestUpdateWithRetryIdempotentReplay`、`TestPersistenceRoundTrip`）、
翻页边界与稳定性（`TestListManifests*`）、非法游标与参数
（`TestListInvalidCursorAndParams`）、调用取消（`TestCancellation`）。
用例日志中以 `input:` / `state:` / `failure:` 前缀标注输入、关键状态与失败分类。
