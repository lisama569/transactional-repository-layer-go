# transactional-repository-layer-go

围绕**清单（Checklist）**与**变更记录（ChangeRecord）**的本地数据访问层，
提供持久化、事务组合、游标分页查询和并发更新能力。纯内存实现，
不依赖任何外部服务，可直接被上层业务复用，也可作为真实存储的参考语义。

## 结构

| 文件 | 职责 |
|---|---|
| `types.go` | 领域类型：`Checklist`、`ChangeRecord`、`Filter`、`PageRequest`/`Page` |
| `errors.go` | 统一错误类型 `Error` 与分类 `ErrorKind` |
| `store.go` | 存储引擎 `Store` 与事务 `Tx`：版本历史、提交边界、幂等去重 |
| `repository.go` | 仓储入口：`ChecklistRepository`、`ChangeRecordRepository`、`WithTx` |
| `cursor.go` | 分页游标编解码（对外不透明） |
| `retry.go` | 冲突重试 `WithRetry` |

## 事务边界

```go
err := store.WithTx(ctx, func(tx *Tx, r Repositories) error {
    if _, err := r.Checklists.Create(ctx, tx, itemA, "idem-key-a"); err != nil {
        return err
    }
    _, err := r.Checklists.Update(ctx, tx, itemB, expectVersion, "")
    return err
})
```

- `fn` 内的所有读写属于同一事务，**提交成功时一起可见**。
- `fn` 返回错误 → 自动 `Rollback`，所有暂存修改被丢弃，全局修订号不变。
- `ctx` 在提交前/提交中被取消 → 返回 `KindCanceled`，修改不生效，
  调用方不会误以为操作已成功。
- 提交时**重新校验**事务内记录的全部期望版本号（乐观锁），
  两个事务交错提交不会造成丢失更新；校验失败整个事务回滚并返回 `KindConflict`。
- 只读事务不推进全局修订号，不会干扰分页快照。

## 分页与筛选规则

- `List` 采用**键序 + 快照**游标：第一页固定当前全局修订号，
  游标（不透明字符串）携带该修订号与上一页末条 ID。
- 翻页期间数据被新增或更新时，后续页仍读取同一快照，
  **不重复、不漏掉**翻页前已满足条件的数据。
- `Filter` 支持按 `Owner`、`Done` 筛选，零值字段不参与过滤。
- `Limit` 合法范围 `[1, 500]`，`0` 表示默认值 50；越界返回 `KindInvalidArgument`。
- 游标格式非法返回 `KindInvalidCursor`，与参数错误**分开识别**。
- `NextCursor` 为空表示没有下一页。

## 并发更新与重试规则

- `Update`/`Delete` 必须携带 `expectVersion`（来自先前读取的 `Version`），
  不一致时返回 `KindConflict`。
- 推荐模式：`WithRetry` 包裹「事务内重新 Get → 改 → 按新版本 Update」，
  仅在 `KindConflict` 时重试，默认最多 3 次（可配置）。
- **幂等键**：`Create`/`Update`/`Delete` 接受 `idempotencyKey`；
  同一键重复执行直接回放首次结果，**不会产生重复变更记录或副作用**，
  提交阶段还有二次去重兜底。
- 每次成功写入产生一条 `ChangeRecord`（含全局修订号），
  可通过 `Changes.ListByChecklist` 审计。

## 错误分类

| Kind | 含义 | 建议处理 |
|---|---|---|
| `not_found` | 记录不存在 | 按业务处理 |
| `conflict` | 版本冲突（乐观锁） | 重读后重试 |
| `invalid_argument` | 参数非法（ID 为空、Limit 越界等） | 修正调用 |
| `invalid_cursor` | 游标非法 | 重新从第一页查 |
| `canceled` | ctx 取消/超时 | 按取消语义处理 |
| `internal` | 未分类的内部错误 | 记录并上报 |

用 `IsKind(err, KindConflict)` / `KindOf(err)` 判断，不要匹配错误字符串。

## 本地验证

```bash
go vet ./...
go test -race -count=1 -v ./...
```

测试全部本地完成，覆盖：事务提交可见/回滚/取消（`store_tx_test.go`）、
翻页边界/翻页期间并发写入/非法游标与参数（`pagination_test.go`）、
20 路并发更新不丢更新/版本冲突/幂等去重/重试耗尽与取消
（`concurrency_test.go`）。用例日志（`go test -v`）包含输入、
关键状态（修订号、版本、页内容）与失败分类。
