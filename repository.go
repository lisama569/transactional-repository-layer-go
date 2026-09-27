package datalayer

import (
	"context"
	"fmt"
	"sort"
)

// ChecklistRepository 清单读写入口，所有方法都在传入的事务内执行。
type ChecklistRepository struct{}

// ChangeRecordRepository 变更记录查询入口。
type ChangeRecordRepository struct{}

// Repositories 聚合一次事务内可用的全部仓储。
type Repositories struct {
	Checklists ChecklistRepository
	Changes    ChangeRecordRepository
}

const (
	maxPageLimit     = 500
	defaultPageLimit = 50
)

// WithTx 在单个事务内执行 fn：fn 返回 nil 且提交成功时修改一起可见；
// fn 返回错误、ctx 取消或提交失败时，所有修改都会被丢弃。
func (s *Store) WithTx(ctx context.Context, fn func(tx *Tx, repos Repositories) error) error {
	tx, err := s.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx, Repositories{}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

// Get 按 ID 读取清单。
func (ChecklistRepository) Get(ctx context.Context, tx *Tx, id string) (Checklist, error) {
	const op = "checklists.Get"
	if err := ctx.Err(); err != nil {
		return Checklist{}, canceledError(op, ctx)
	}
	if id == "" {
		return Checklist{}, newError(KindInvalidArgument, op, "id is required", nil)
	}
	item, ok, err := tx.get(id)
	if err != nil {
		return Checklist{}, err
	}
	if !ok {
		return Checklist{}, newError(KindNotFound, op, fmt.Sprintf("checklist %q not found", id), nil)
	}
	return item, nil
}

// Create 新建清单并产生一条变更记录；idempotencyKey 非空时保证重复调用无副作用。
func (ChecklistRepository) Create(ctx context.Context, tx *Tx, item Checklist, idempotencyKey string) (Checklist, error) {
	const op = "checklists.Create"
	if err := ctx.Err(); err != nil {
		return Checklist{}, canceledError(op, ctx)
	}
	if item.ID == "" || item.Title == "" {
		return Checklist{}, newError(KindInvalidArgument, op, "id and title are required", nil)
	}
	if idempotencyKey != "" {
		if r, ok := tx.idemLookup(idempotencyKey); ok {
			return r.item, nil // 幂等命中：直接回放首次结果
		}
	}
	if _, ok, err := tx.get(item.ID); err != nil {
		return Checklist{}, err
	} else if ok {
		return Checklist{}, newError(KindInvalidArgument, op, fmt.Sprintf("checklist %q already exists", item.ID), nil)
	}
	item.Version = 0 // 由提交时统一赋值为 1
	tx.stage(item, 0, ChangeRecord{
		ChecklistID:    item.ID,
		Op:             "create",
		IdempotencyKey: idempotencyKey,
	})
	return item, nil
}

// Update 带版本号的条件更新：expectVersion 与当前版本不一致时返回 KindConflict。
func (ChecklistRepository) Update(ctx context.Context, tx *Tx, item Checklist, expectVersion int64, idempotencyKey string) (Checklist, error) {
	const op = "checklists.Update"
	if err := ctx.Err(); err != nil {
		return Checklist{}, canceledError(op, ctx)
	}
	if item.ID == "" {
		return Checklist{}, newError(KindInvalidArgument, op, "id is required", nil)
	}
	if expectVersion <= 0 {
		return Checklist{}, newError(KindInvalidArgument, op, "expectVersion must be positive", nil)
	}
	if idempotencyKey != "" {
		if r, ok := tx.idemLookup(idempotencyKey); ok {
			return r.item, nil
		}
	}
	cur, ok, err := tx.get(item.ID)
	if err != nil {
		return Checklist{}, err
	}
	if !ok {
		return Checklist{}, newError(KindNotFound, op, fmt.Sprintf("checklist %q not found", item.ID), nil)
	}
	if cur.Version != expectVersion {
		return Checklist{}, newError(KindConflict, op,
			fmt.Sprintf("checklist %q version mismatch: expect %d, got %d", item.ID, expectVersion, cur.Version), nil)
	}
	item.CreatedAt = cur.CreatedAt
	tx.stage(item, expectVersion, ChangeRecord{
		ChecklistID:    item.ID,
		Op:             "update",
		IdempotencyKey: idempotencyKey,
	})
	return item, nil
}

// Delete 带版本号的条件删除。
func (ChecklistRepository) Delete(ctx context.Context, tx *Tx, id string, expectVersion int64, idempotencyKey string) error {
	const op = "checklists.Delete"
	if err := ctx.Err(); err != nil {
		return canceledError(op, ctx)
	}
	if id == "" {
		return newError(KindInvalidArgument, op, "id is required", nil)
	}
	if expectVersion <= 0 {
		return newError(KindInvalidArgument, op, "expectVersion must be positive", nil)
	}
	if idempotencyKey != "" {
		if _, ok := tx.idemLookup(idempotencyKey); ok {
			return nil // 已执行过，直接成功
		}
	}
	cur, ok, err := tx.get(id)
	if err != nil {
		return err
	}
	if !ok {
		return newError(KindNotFound, op, fmt.Sprintf("checklist %q not found", id), nil)
	}
	if cur.Version != expectVersion {
		return newError(KindConflict, op,
			fmt.Sprintf("checklist %q version mismatch: expect %d, got %d", id, expectVersion, cur.Version), nil)
	}
	tx.stageDelete(id, expectVersion, ChangeRecord{
		ChecklistID:    id,
		Op:             "delete",
		IdempotencyKey: idempotencyKey,
	})
	return nil
}

// List 按条件筛选并按键序游标翻页；结果基于游标记录的快照修订号，
// 翻页期间的新增/更新不会造成重复或遗漏。
func (ChecklistRepository) List(ctx context.Context, tx *Tx, filter Filter, page PageRequest) (Page[Checklist], error) {
	const op = "checklists.List"
	if err := ctx.Err(); err != nil {
		return Page[Checklist]{}, canceledError(op, ctx)
	}
	limit, err := normalizeLimit(op, page.Limit)
	if err != nil {
		return Page[Checklist]{}, err
	}
	var cur cursor
	if page.Cursor == "" {
		cur.Revision = tx.store.Revision() // 第一页：固定当前快照
	} else {
		cur, err = decodeCursor(page.Cursor)
		if err != nil {
			return Page[Checklist]{}, err
		}
	}

	items := tx.store.snapshotAt(cur.Revision)
	matched := items[:0]
	for _, it := range items {
		if !matchFilter(it, filter) {
			continue
		}
		if it.ID > cur.LastID { // 键序翻页：只取上一页之后的记录
			matched = append(matched, it)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })

	out := Page[Checklist]{Items: []Checklist{}}
	if len(matched) > limit {
		matched = matched[:limit]
		out.NextCursor = encodeCursor(cursor{Revision: cur.Revision, LastID: matched[len(matched)-1].ID})
	}
	out.Items = append(out.Items, matched...)
	return out, nil
}

// ListByChecklist 查询某清单的变更记录（按 ID 升序翻页）。
func (ChangeRecordRepository) ListByChecklist(ctx context.Context, tx *Tx, checklistID string, page PageRequest) (Page[ChangeRecord], error) {
	const op = "changes.ListByChecklist"
	if err := ctx.Err(); err != nil {
		return Page[ChangeRecord]{}, canceledError(op, ctx)
	}
	if checklistID == "" {
		return Page[ChangeRecord]{}, newError(KindInvalidArgument, op, "checklistID is required", nil)
	}
	limit, err := normalizeLimit(op, page.Limit)
	if err != nil {
		return Page[ChangeRecord]{}, err
	}
	var lastID string
	if page.Cursor != "" {
		cur, err := decodeCursor(page.Cursor)
		if err != nil {
			return Page[ChangeRecord]{}, err
		}
		lastID = cur.LastID
	}
	// 多取一条判断是否还有下一页。
	got := tx.store.changesSince(checklistID, lastID, limit+1)
	out := Page[ChangeRecord]{Items: []ChangeRecord{}}
	if len(got) > limit {
		got = got[:limit]
		out.NextCursor = encodeCursor(cursor{Revision: tx.store.Revision(), LastID: got[len(got)-1].ID})
	}
	out.Items = append(out.Items, got...)
	return out, nil
}

func normalizeLimit(op string, limit int) (int, error) {
	if limit == 0 {
		return defaultPageLimit, nil
	}
	if limit < 0 || limit > maxPageLimit {
		return 0, newError(KindInvalidArgument, op,
			fmt.Sprintf("limit must be in [1, %d], got %d", maxPageLimit, limit), nil)
	}
	return limit, nil
}

func matchFilter(it Checklist, f Filter) bool {
	if f.Owner != "" && it.Owner != f.Owner {
		return false
	}
	if f.Done != nil && it.Done != *f.Done {
		return false
	}
	return true
}
