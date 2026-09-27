package datalayer

import (
	"context"
	"errors"
	"testing"
)

func TestWithTxCommitVisible(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		if _, err := r.Checklists.Create(ctx, tx, Checklist{ID: "a", Title: "A"}, ""); err != nil {
			return err
		}
		_, err := r.Checklists.Create(ctx, tx, Checklist{ID: "b", Title: "B"}, "")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	t.Logf("committed revision=%d", s.Revision())

	var got Checklist
	err = s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		var err error
		got, err = r.Checklists.Get(ctx, tx, "a")
		return err
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Title != "A" || got.Version != 1 {
		t.Fatalf("unexpected item: %+v", got)
	}
	t.Logf("read back: id=%s version=%d", got.ID, got.Version)
}

func TestWithTxRollbackOnError(t *testing.T) {
	s := NewStore()
	ctx := context.Background()
	boom := errors.New("boom")

	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		if _, err := r.Checklists.Create(ctx, tx, Checklist{ID: "a", Title: "A"}, ""); err != nil {
			return err
		}
		t.Logf("staged create id=a, now failing fn")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	t.Logf("fn error classified as kind=%s", KindOf(err))

	// 回滚后不得留下半成品。
	err = s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.Get(ctx, tx, "a")
		return err
	})
	if !IsKind(err, KindNotFound) {
		t.Fatalf("want not_found after rollback, got %v", err)
	}
	if rev := s.Revision(); rev != 0 {
		t.Fatalf("revision should stay 0 after rollback, got %d", rev)
	}
	t.Logf("after rollback: id=a not found, revision=%d", s.Revision())
}

func TestWithTxCanceledContext(t *testing.T) {
	s := NewStore()
	ctx, cancel := context.WithCancel(context.Background())

	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		if _, err := r.Checklists.Create(ctx, tx, Checklist{ID: "a", Title: "A"}, ""); err != nil {
			return err
		}
		cancel() // 提交前取消
		t.Logf("canceled ctx before commit")
		return nil
	})
	if !IsKind(err, KindCanceled) {
		t.Fatalf("want canceled, got %v (kind=%s)", err, KindOf(err))
	}
	t.Logf("commit after cancel classified as kind=%s", KindOf(err))

	err = s.WithTx(context.Background(), func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.Get(context.Background(), tx, "a")
		return err
	})
	if !IsKind(err, KindNotFound) {
		t.Fatalf("canceled tx must not be visible, got %v", err)
	}
}

func TestBeginWithCanceledContext(t *testing.T) {
	s := NewStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Begin(ctx); !IsKind(err, KindCanceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	t.Logf("begin with canceled ctx classified as kind=canceled")
}
