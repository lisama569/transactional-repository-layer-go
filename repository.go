package txrepo

import "context"

// Repository 面向业务的入口：组合事务、分页查询与重试策略。
type Repository struct {
	store *Store
}

// NewRepository 基于存储构造仓储。
func NewRepository(s *Store) *Repository {
	return &Repository{store: s}
}

// WithTx 在单个事务中执行 fn：
//   - fn 返回 nil 且提交成功，全部写入一起可见；
//   - fn 返回错误、ctx 被取消或提交失败时，全部回滚，不留半成品。
func (r *Repository) WithTx(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := r.store.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return &Error{Kind: KindCanceled, Op: "WithTx", Msg: "canceled before commit", Err: err}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// Get 读取单个清单。
func (r *Repository) Get(ctx context.Context, id string) (Manifest, error) {
	return r.store.getManifest(ctx, id)
}

// ListManifests 按筛选条件与游标分页列出清单。
func (r *Repository) ListManifests(ctx context.Context, f Filter, req PageRequest) (Page[Manifest], error) {
	return r.store.listManifests(ctx, f, req)
}

// ListChanges 分页列出某清单的变更记录。
func (r *Repository) ListChanges(ctx context.Context, manifestID string, req PageRequest) (Page[ChangeRecord], error) {
	return r.store.listChanges(ctx, manifestID, req)
}

// UpdateWithRetry 带版本号条件更新：读取-修改-写回，遇版本冲突按 maxRetries 重试，
// 避免并发写入丢失更新。idempotencyKey 保证同一逻辑操作重复执行
// （包括调用方层面的重试）不会产生重复变更记录。
// mutate 返回修改后的清单与变更说明。
func (r *Repository) UpdateWithRetry(ctx context.Context, id, idempotencyKey string, maxRetries int, mutate func(m Manifest) (Manifest, string, error)) (Manifest, error) {
	if id == "" || mutate == nil {
		return Manifest{}, &Error{Kind: KindInvalidArgument, Op: "UpdateWithRetry", Msg: "id and mutate are required", Err: ErrInvalidArgument}
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return Manifest{}, &Error{Kind: KindCanceled, Op: "UpdateWithRetry", Msg: "canceled", Err: err}
		}
		cur, err := r.store.getManifest(ctx, id)
		if err != nil {
			return Manifest{}, err
		}
		next, detail, err := mutate(cur)
		if err != nil {
			return Manifest{}, err
		}
		next.ID = id
		err = r.WithTx(ctx, func(tx *Tx) error {
			return tx.Update(ctx, next, cur.Version, idempotencyKey, detail)
		})
		if err == nil {
			return r.store.getManifest(ctx, id)
		}
		// 仅版本冲突可重试；其余错误（取消、参数、未找到等）直接返回。
		if KindOf(err) != KindConflict {
			return Manifest{}, err
		}
		lastErr = err
	}
	return Manifest{}, &Error{Kind: KindConflict, Op: "UpdateWithRetry",
		Msg: "exhausted retries on version conflict", Err: lastErr}
}
