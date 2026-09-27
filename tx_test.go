package txrepo

import (
	"context"
	"errors"
	"testing"
)

func newRepo(t *testing.T) *Repository {
	t.Helper()
	store, err := Open("") // 纯内存
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return NewRepository(store)
}

func TestWithTxCommitAllVisible(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	t.Logf("input: 单事务创建 m1、m2")
	err := repo.WithTx(ctx, func(tx *Tx) error {
		if err := tx.Create(ctx, Manifest{ID: "m1", Name: "alpha", Status: "open"}, "idem-c1"); err != nil {
			return err
		}
		return tx.Create(ctx, Manifest{ID: "m2", Name: "beta", Status: "open"}, "idem-c2")
	})
	if err != nil {
		t.Fatalf("WithTx: %v (kind=%s)", err, KindOf(err))
	}
	for _, id := range []string{"m1", "m2"} {
		m, err := repo.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		t.Logf("state: %s version=%d seq=%d", id, m.Version, m.Seq)
	}
}

func TestWithTxRollbackOnError(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	boom := errors.New("boom")

	t.Logf("input: 事务内先建 m1，再返回错误触发回滚")
	err := repo.WithTx(ctx, func(tx *Tx) error {
		if err := tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, ""); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if _, err := repo.Get(ctx, "m1"); KindOf(err) != KindNotFound {
		t.Fatalf("rollback 后 m1 不应存在, err=%v kind=%s", err, KindOf(err))
	}
	t.Logf("state: 回滚生效，m1 不存在 (kind=%s)", KindOf(err))
}

func TestTxCommitConflictLeavesNothing(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	if err := repo.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, "")
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t.Logf("input: 事务基于过期版本更新 m1，同时创建 m2")
	err := repo.WithTx(ctx, func(tx *Tx) error {
		if err := tx.Create(ctx, Manifest{ID: "m2", Name: "beta"}, ""); err != nil {
			return err
		}
		m, _ := tx.Get(ctx, "m1")
		m.Status = "closed"
		return tx.Update(ctx, m, 99, "", "stale update") // 期望版本错误
	})
	t.Logf("failure: kind=%s err=%v", KindOf(err), err)
	if KindOf(err) != KindConflict {
		t.Fatalf("want conflict, got %s (%v)", KindOf(err), err)
	}
	if _, err := repo.Get(ctx, "m2"); KindOf(err) != KindNotFound {
		t.Fatalf("提交失败不应留下半成品 m2, err=%v", err)
	}
	m, _ := repo.Get(ctx, "m1")
	if m.Version != 1 || m.Status != "" {
		t.Fatalf("m1 不应被修改: %+v", m)
	}
	t.Logf("state: m1 version=%d 未变，m2 不存在", m.Version)
}

func TestTxReadYourWrites(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	err := repo.WithTx(ctx, func(tx *Tx) error {
		if err := tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, ""); err != nil {
			return err
		}
		m, err := tx.Get(ctx, "m1")
		if err != nil {
			t.Fatalf("tx 内应能读到未提交写入: %v", err)
		}
		t.Logf("state: tx 内读到 m1 name=%q", m.Name)
		m.Status = "open"
		return tx.Update(ctx, m, m.Version, "", "set status")
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	m, _ := repo.Get(ctx, "m1")
	if m.Status != "open" || m.Version != 2 {
		t.Fatalf("unexpected: %+v", m)
	}
}

func TestTxClosedAfterCommit(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	tx, err := repo.store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	err = tx.Create(ctx, Manifest{ID: "x", Name: "x"}, "")
	t.Logf("failure: 提交后再写入 kind=%s err=%v", KindOf(err), err)
	if KindOf(err) != KindTxClosed {
		t.Fatalf("want tx_closed, got %s", KindOf(err))
	}
}
