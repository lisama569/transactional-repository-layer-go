package datalayer

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// versionedItem 是清单的一个历史版本，revision 为产生该版本的提交修订号。
type versionedItem struct {
	item    Checklist
	revision int64
	deleted bool
}

// idemResult 记录幂等键首次执行的结果，用于去重与结果回放。
type idemResult struct {
	item   Checklist
	change ChangeRecord
}

// Store 是本地存储引擎，负责持久化、事务边界与全局修订号。
// 所有方法并发安全。
type Store struct {
	mu       sync.RWMutex
	revision int64 // 全局修订号，每次成功提交 +1
	seq      int64 // 变更记录 ID 发号器
	history  map[string][]versionedItem
	changes  []*ChangeRecord
	idem     map[string]idemResult // 幂等键 -> 首次执行结果
}

// NewStore 创建空的本地存储。
func NewStore() *Store {
	return &Store{
		history: make(map[string][]versionedItem),
		idem:    make(map[string]idemResult),
	}
}

// Tx 是一次事务的上下文：读写在提交前对其他事务不可见，
// 提交时整体生效；任何失败都会丢弃全部暂存修改。
type Tx struct {
	store   *Store
	done    bool
	staged  map[string]stagedWrite // 暂存的写入/删除
	expect  map[string]int64       // 乐观锁：提交时重新校验的版本号
	pending []*ChangeRecord        // 暂存的变更记录
}

type stagedWrite struct {
	item    Checklist
	deleted bool
}

// Begin 开启事务。ctx 已取消时立即失败。
func (s *Store) Begin(ctx context.Context) (*Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, canceledError("store.Begin", ctx)
	}
	return &Tx{
		store:  s,
		staged: make(map[string]stagedWrite),
		expect: make(map[string]int64),
	}, nil
}

// Commit 提交事务：在持锁状态下重新校验版本号，
// 通过后把所有暂存修改一次性生效；任一校验失败则全部丢弃。
func (t *Tx) Commit(ctx context.Context) error {
	const op = "tx.Commit"
	if t.done {
		return newError(KindInternal, op, "transaction already finished", nil)
	}
	t.done = true
	if err := ctx.Err(); err != nil {
		return canceledError(op, ctx)
	}

	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return canceledError(op, ctx)
	}

	// 只读事务不产生新修订号，避免污染分页快照。
	if len(t.staged) == 0 && len(t.pending) == 0 {
		return nil
	}

	// 提交时重新校验乐观锁，避免两个事务之间的丢失更新。
	for id, want := range t.expect {
		if got := s.currentVersionLocked(id); got != want {
			return newError(KindConflict, op,
				fmt.Sprintf("checklist %q version changed: expect %d, got %d", id, want, got), nil)
		}
	}

	s.revision++
	now := time.Now()
	for id, w := range t.staged {
		item := w.item
		item.Version = s.currentVersionLocked(id) + 1
		item.UpdatedAt = now
		if item.CreatedAt.IsZero() {
			item.CreatedAt = now
		}
		s.history[id] = append(s.history[id], versionedItem{item: item, revision: s.revision, deleted: w.deleted})
	}
	for _, c := range t.pending {
		if c.IdempotencyKey != "" {
			if _, dup := s.idem[c.IdempotencyKey]; dup {
				continue // 幂等键已存在：跳过，不产生重复变更记录
			}
		}
		s.seq++
		rec := *c
		rec.ID = fmt.Sprintf("chg-%d", s.seq)
		rec.Revision = s.revision
		rec.CreatedAt = now
		s.changes = append(s.changes, &rec)
		if rec.IdempotencyKey != "" {
			item := Checklist{}
			if w, ok := t.staged[rec.ChecklistID]; ok {
				item = w.item
			}
			s.idem[rec.IdempotencyKey] = idemResult{item: item, change: rec}
		}
	}
	return nil
}

// Rollback 丢弃事务的所有暂存修改；可安全重复调用。
func (t *Tx) Rollback() {
	t.done = true
	t.staged = nil
	t.pending = nil
	t.expect = nil
}

// Revision 返回当前全局修订号（只读视图）。
func (s *Store) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// currentVersionLocked 返回已提交的最新版本号，不存在时返回 0。调用方须持锁。
func (s *Store) currentVersionLocked(id string) int64 {
	vs := s.history[id]
	if len(vs) == 0 {
		return 0
	}
	return vs[len(vs)-1].item.Version
}

// get 在事务内读取：先看暂存，再看已提交的最新版本。
func (t *Tx) get(id string) (Checklist, bool, error) {
	if w, ok := t.staged[id]; ok {
		return w.item, !w.deleted, nil
	}
	s := t.store
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs := s.history[id]
	if len(vs) == 0 || vs[len(vs)-1].deleted {
		return Checklist{}, false, nil
	}
	return vs[len(vs)-1].item, true, nil
}

// idemLookup 查询幂等键是否已有提交结果。
func (t *Tx) idemLookup(key string) (idemResult, bool) {
	s := t.store
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.idem[key]
	return r, ok
}

// stage 暂存一次写入，并记录提交时要校验的期望版本（新建传 0）。
func (t *Tx) stage(item Checklist, expectVersion int64, change ChangeRecord) {
	t.staged[item.ID] = stagedWrite{item: item}
	t.expect[item.ID] = expectVersion
	t.pending = append(t.pending, &change)
}

// stageDelete 暂存一次删除。
func (t *Tx) stageDelete(id string, expectVersion int64, change ChangeRecord) {
	t.staged[id] = stagedWrite{item: Checklist{ID: id}, deleted: true}
	t.expect[id] = expectVersion
	t.pending = append(t.pending, &change)
}

// snapshotAt 返回指定修订号下的全部可见清单（deleted 除外）。
// revision <= 0 表示当前最新版本。
func (s *Store) snapshotAt(revision int64) []Checklist {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Checklist, 0, len(s.history))
	for id, vs := range s.history {
		var v *versionedItem
		for i := range vs {
			if revision <= 0 || vs[i].revision <= revision {
				v = &vs[i]
			}
		}
		if v != nil && !v.deleted {
			_ = id
			out = append(out, v.item)
		}
	}
	return out
}

// changesSince 返回指定清单、ID 大于 afterID 的变更记录（按 ID 升序）。
func (s *Store) changesSince(checklistID, afterID string, limit int) []ChangeRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ChangeRecord, 0, limit)
	for _, c := range s.changes {
		if c.ChecklistID != checklistID || c.ID <= afterID {
			continue
		}
		out = append(out, *c)
		if len(out) == limit {
			break
		}
	}
	return out
}
