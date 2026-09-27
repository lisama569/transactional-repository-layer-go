package txrepo

import (
	"context"
	"fmt"
	"sync"
)

// mutation 事务内的一次待提交写入。
type mutation struct {
	manifestID     string
	op             ChangeOp
	expectVersion  uint64 // 条件更新的期望版本；create 时为 0
	after          *Manifest
	idempotencyKey string
	detail         string
}

// txBatch 一次事务提交的完整载荷。
type txBatch struct {
	mutations []mutation
}

// Tx 事务句柄：事务内的读写先落在暂存区，Commit 时原子生效。
// Tx 不是并发安全的，请勿在多个 goroutine 间共享。
type Tx struct {
	store   *Store
	base    snapshot // 事务开始时的读视图
	mu      sync.Mutex
	pending []mutation
	staged  map[string]*Manifest // 读己之写视图；nil 值表示已删除
	closed  bool
}

// Begin 开启一个事务，并固定事务开始时的读视图。
func (s *Store) Begin(ctx context.Context) (*Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{Kind: KindCanceled, Op: "Begin", Msg: "canceled", Err: err}
	}
	return &Tx{
		store:  s,
		base:   s.readSnapshot(),
		staged: map[string]*Manifest{},
	}, nil
}

func (t *Tx) checkOpen(op string) error {
	if t.closed {
		return &Error{Kind: KindTxClosed, Op: op, Msg: "transaction already committed or rolled back", Err: ErrTxClosed}
	}
	return nil
}

// Get 在事务内读取清单（可见本事务未提交的写入）。
func (t *Tx) Get(ctx context.Context, id string) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, &Error{Kind: KindCanceled, Op: "Tx.Get", Msg: "canceled", Err: err}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpen("Tx.Get"); err != nil {
		return Manifest{}, err
	}
	if m, ok := t.staged[id]; ok {
		if m == nil {
			return Manifest{}, &Error{Kind: KindNotFound, Op: "Tx.Get", Msg: fmt.Sprintf("manifest %q deleted in tx", id), Err: ErrNotFound}
		}
		return *m, nil
	}
	m, ok := t.base.manifests[id]
	if !ok {
		return Manifest{}, &Error{Kind: KindNotFound, Op: "Tx.Get", Msg: fmt.Sprintf("manifest %q not found", id), Err: ErrNotFound}
	}
	return m, nil
}

// Create 在事务内新建清单。ID 为空或缺 Name 视为参数错误。
func (t *Tx) Create(ctx context.Context, m Manifest, idempotencyKey string) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Op: "Tx.Create", Msg: "canceled", Err: err}
	}
	if m.ID == "" || m.Name == "" {
		return &Error{Kind: KindInvalidArgument, Op: "Tx.Create", Msg: "id and name are required", Err: ErrInvalidArgument}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpen("Tx.Create"); err != nil {
		return err
	}
	if _, ok := t.staged[m.ID]; ok {
		return &Error{Kind: KindConflict, Op: "Tx.Create", Msg: fmt.Sprintf("manifest %q already staged", m.ID), Err: ErrConflict}
	}
	// 带幂等键时，存在性冲突推迟到提交期判定：若该键已应用过则去重为 no-op，
	// 否则提交时仍会报冲突。
	if _, ok := t.base.manifests[m.ID]; ok && idempotencyKey == "" {
		return &Error{Kind: KindConflict, Op: "Tx.Create", Msg: fmt.Sprintf("manifest %q already exists", m.ID), Err: ErrConflict}
	}
	cp := m
	cp.Version = 0
	t.staged[m.ID] = &cp
	t.pending = append(t.pending, mutation{
		manifestID:     m.ID,
		op:             ChangeOpCreate,
		expectVersion:  0,
		after:          &cp,
		idempotencyKey: idempotencyKey,
		detail:         "create " + m.ID,
	})
	return nil
}

// Update 在事务内按期望版本更新清单（乐观并发控制）。
func (t *Tx) Update(ctx context.Context, m Manifest, expectVersion uint64, idempotencyKey, detail string) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Op: "Tx.Update", Msg: "canceled", Err: err}
	}
	if m.ID == "" {
		return &Error{Kind: KindInvalidArgument, Op: "Tx.Update", Msg: "id is required", Err: ErrInvalidArgument}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpen("Tx.Update"); err != nil {
		return err
	}
	// 基于读己之写视图校验版本，尽早发现冲突。
	cur, err := t.getLocked(m.ID)
	if err != nil {
		return err
	}
	if cur.Version != expectVersion {
		return &Error{Kind: KindConflict, Op: "Tx.Update",
			Msg: fmt.Sprintf("manifest %q version mismatch: expect %d, actual %d", m.ID, expectVersion, cur.Version),
			Err: ErrConflict}
	}
	cp := m
	t.staged[m.ID] = &cp
	t.pending = append(t.pending, mutation{
		manifestID:     m.ID,
		op:             ChangeOpUpdate,
		expectVersion:  expectVersion,
		after:          &cp,
		idempotencyKey: idempotencyKey,
		detail:         detail,
	})
	return nil
}

// Delete 在事务内按期望版本删除清单。
func (t *Tx) Delete(ctx context.Context, id string, expectVersion uint64, idempotencyKey string) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Op: "Tx.Delete", Msg: "canceled", Err: err}
	}
	if id == "" {
		return &Error{Kind: KindInvalidArgument, Op: "Tx.Delete", Msg: "id is required", Err: ErrInvalidArgument}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpen("Tx.Delete"); err != nil {
		return err
	}
	cur, err := t.getLocked(id)
	if err != nil {
		return err
	}
	if cur.Version != expectVersion {
		return &Error{Kind: KindConflict, Op: "Tx.Delete",
			Msg: fmt.Sprintf("manifest %q version mismatch: expect %d, actual %d", id, expectVersion, cur.Version),
			Err: ErrConflict}
	}
	t.staged[id] = nil
	t.pending = append(t.pending, mutation{
		manifestID:     id,
		op:             ChangeOpDelete,
		expectVersion:  expectVersion,
		idempotencyKey: idempotencyKey,
		detail:         "delete " + id,
	})
	return nil
}

// getLocked 在持锁下读取读己之写视图。
func (t *Tx) getLocked(id string) (Manifest, error) {
	if m, ok := t.staged[id]; ok {
		if m == nil {
			return Manifest{}, &Error{Kind: KindNotFound, Op: "Tx", Msg: fmt.Sprintf("manifest %q deleted in tx", id), Err: ErrNotFound}
		}
		return *m, nil
	}
	m, ok := t.base.manifests[id]
	if !ok {
		return Manifest{}, &Error{Kind: KindNotFound, Op: "Tx", Msg: fmt.Sprintf("manifest %q not found", id), Err: ErrNotFound}
	}
	return m, nil
}

// Commit 原子提交全部暂存写入；校验失败、取消或落盘失败时全部不生效。
// Commit 之后事务即关闭，无论成败。
func (t *Tx) Commit(ctx context.Context) error {
	t.mu.Lock()
	if err := t.checkOpen("Tx.Commit"); err != nil {
		t.mu.Unlock()
		return err
	}
	t.closed = true
	batch := txBatch{mutations: append([]mutation(nil), t.pending...)}
	t.mu.Unlock()
	return t.store.apply(ctx, batch)
}

// Rollback 显式放弃事务，丢弃全部暂存写入。
func (t *Tx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.checkOpen("Tx.Rollback"); err != nil {
		return err
	}
	t.closed = true
	t.pending = nil
	t.staged = map[string]*Manifest{}
	return nil
}
