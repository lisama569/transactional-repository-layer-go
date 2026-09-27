package datalayer

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentUpdatesNoLostUpdate(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.Create(ctx, tx, Checklist{ID: "c", Title: "counter"}, "")
		return err
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 20 个协程同时对同一记录做 读-改-写，靠版本号 + 重试保证不丢更新。
	const workers = 20
	var wg sync.WaitGroup
	conflicts := make(chan int, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			attempts := 0
			err := WithRetry(ctx, RetryOptions{MaxAttempts: 100}, func(ctx context.Context) error {
				attempts++
				return s.WithTx(ctx, func(tx *Tx, r Repositories) error {
					it, err := r.Checklists.Get(ctx, tx, "c")
					if err != nil {
						return err
					}
					it.Title = fmt.Sprintf("w%d", w)
					it.Done = !it.Done
					_, err = r.Checklists.Update(ctx, tx, it, it.Version, "")
					return err
				})
			})
			if err != nil {
				t.Errorf("worker %d: %v (kind=%s)", w, err, KindOf(err))
			}
			conflicts <- attempts
		}(w)
	}
	wg.Wait()
	close(conflicts)

	total := 0
	for a := range conflicts {
		total += a
	}
	t.Logf("workers=%d total attempts=%d (conflicts retried=%d)", workers, total, total-workers)

	var final Checklist
	var changes Page[ChangeRecord]
	err = s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		var err error
		final, err = r.Checklists.Get(ctx, tx, "c")
		if err != nil {
			return err
		}
		changes, err = r.Changes.ListByChecklist(ctx, tx, "c", PageRequest{Limit: 100})
		return err
	})
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	// 1 次 create + 20 次成功 update = 21 个版本、21 条变更记录，一个都不能丢。
	if final.Version != 1+workers {
		t.Fatalf("lost update: version=%d want %d", final.Version, 1+workers)
	}
	if len(changes.Items) != 1+workers {
		t.Fatalf("change records=%d want %d", len(changes.Items), 1+workers)
	}
	t.Logf("final version=%d change records=%d", final.Version, len(changes.Items))
}

func TestVersionConflictDetected(t *testing.T) {
	s := NewStore()
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.Create(ctx, tx, Checklist{ID: "v", Title: "v"}, "")
		return err
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 用过期版本号更新：必须识别为 KindConflict。
	err = s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		it, err := r.Checklists.Get(ctx, tx, "v")
		if err != nil {
			return err
		}
		it.Title = "stale"
		_, err = r.Checklists.Update(ctx, tx, it, it.Version+9, "")
		return err
	})
	if !IsKind(err, KindConflict) {
		t.Fatalf("want conflict, got %v (kind=%s)", err, KindOf(err))
	}
	t.Logf("stale version update classified as kind=%s", KindOf(err))
}

func TestIdempotentCreateNoDuplicateChange(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	do := func() error {
		return s.WithTx(ctx, func(tx *Tx, r Repositories) error {
			_, err := r.Checklists.Create(ctx, tx, Checklist{ID: "idem-1", Title: "once"}, "key-1")
			return err
		})
	}
	if err := do(); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := do(); err != nil { // 同一幂等键重复执行
		t.Fatalf("second: %v", err)
	}
	t.Logf("executed create twice with idempotency key key-1")

	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		changes, err := r.Changes.ListByChecklist(ctx, tx, "idem-1", PageRequest{Limit: 10})
		if err != nil {
			return err
		}
		t.Logf("change records for idem-1: %d", len(changes.Items))
		if len(changes.Items) != 1 {
			return fmt.Errorf("duplicate change records: %d", len(changes.Items))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithRetryExhaustsOnPersistentConflict(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := WithRetry(ctx, RetryOptions{MaxAttempts: 3}, func(ctx context.Context) error {
		calls++
		return newError(KindConflict, "test", "always conflict", nil)
	})
	if !IsKind(err, KindConflict) || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	t.Logf("persistent conflict: attempts=%d final kind=%s", calls, KindOf(err))
}

func TestWithRetryCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := WithRetry(ctx, RetryOptions{}, func(ctx context.Context) error {
		t.Fatal("op should not run when ctx is canceled")
		return nil
	})
	if !IsKind(err, KindCanceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	t.Logf("retry with canceled ctx classified as kind=%s", KindOf(err))
}
