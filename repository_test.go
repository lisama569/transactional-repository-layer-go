package txrepo

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentUpdatesNoLostUpdate(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	if err := repo.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "counter", Name: "counter", Attributes: map[string]string{"n": "0"}}, "")
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const perWorker = 5
	t.Logf("input: %d 个并发 worker，每个对同一清单做 %d 次读-改-写", workers, perWorker)
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("idem-w%d-%d", w, i)
				_, err := repo.UpdateWithRetry(ctx, "counter", key, 20, func(m Manifest) (Manifest, string, error) {
					n, _ := strconv.Atoi(m.Attributes["n"])
					m.Attributes = map[string]string{"n": strconv.Itoa(n + 1)}
					return m, "increment", nil
				})
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %w", w, i, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发更新失败: %v (kind=%s)", err, KindOf(err))
	}

	m, err := repo.Get(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(m.Attributes["n"])
	t.Logf("state: 最终计数 n=%d version=%d", n, m.Version)
	if n != workers*perWorker {
		t.Fatalf("丢失更新: want %d, got %d", workers*perWorker, n)
	}
	// 变更记录数 = 1 次创建 + 每次成功更新 1 条，且无重复。
	p, err := repo.ListChanges(ctx, "counter", PageRequest{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1+workers*perWorker {
		t.Fatalf("变更记录数不符: want %d, got %d", 1+workers*perWorker, len(p.Items))
	}
}

func TestUpdateWithRetryIdempotentReplay(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	if err := repo.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, "")
	}); err != nil {
		t.Fatal(err)
	}

	mutate := func(m Manifest) (Manifest, string, error) {
		m.Status = "done"
		return m, "mark done", nil
	}
	t.Logf("input: 用同一幂等键重复执行同一更新 3 次")
	for i := 0; i < 3; i++ {
		if _, err := repo.UpdateWithRetry(ctx, "m1", "idem-replay", 5, mutate); err != nil {
			t.Fatalf("第 %d 次执行: %v (kind=%s)", i, err, KindOf(err))
		}
	}
	p, err := repo.ListChanges(ctx, "m1", PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("state: 变更记录数=%d（1 创建 + 1 更新，重放去重）", len(p.Items))
	if len(p.Items) != 2 {
		t.Fatalf("重复执行产生了副作用: want 2 records, got %d", len(p.Items))
	}
	m, _ := repo.Get(ctx, "m1")
	if m.Version != 2 || m.Status != "done" {
		t.Fatalf("unexpected: %+v", m)
	}
}

func TestCancellation(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	if err := repo.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, "")
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("已取消的ctx不能开启事务", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		err := repo.WithTx(cctx, func(tx *Tx) error {
			return tx.Create(cctx, Manifest{ID: "m2", Name: "beta"}, "")
		})
		t.Logf("failure: kind=%s err=%v", KindOf(err), err)
		if KindOf(err) != KindCanceled {
			t.Fatalf("want canceled, got %s", KindOf(err))
		}
		if _, err := repo.Get(ctx, "m2"); KindOf(err) != KindNotFound {
			t.Fatalf("取消后不应留下 m2")
		}
	})

	t.Run("提交前取消则回滚", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		err := repo.WithTx(cctx, func(tx *Tx) error {
			if err := tx.Create(cctx, Manifest{ID: "m3", Name: "gamma"}, ""); err != nil {
				return err
			}
			cancel() // fn 返回 nil 后、提交前取消
			return nil
		})
		t.Logf("failure: kind=%s err=%v", KindOf(err), err)
		if KindOf(err) != KindCanceled {
			t.Fatalf("want canceled, got %s", KindOf(err))
		}
		if _, err := repo.Get(ctx, "m3"); KindOf(err) != KindNotFound {
			t.Fatalf("取消后不应留下 m3")
		}
	})

	t.Run("取消的分页与更新", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := repo.ListManifests(cctx, Filter{}, PageRequest{Limit: 1}); KindOf(err) != KindCanceled {
			t.Fatalf("list: want canceled, got %s", KindOf(err))
		}
		_, err := repo.UpdateWithRetry(cctx, "m1", "k", 3, func(m Manifest) (Manifest, string, error) {
			return m, "", nil
		})
		t.Logf("failure: update kind=%s err=%v", KindOf(err), err)
		if KindOf(err) != KindCanceled {
			t.Fatalf("update: want canceled, got %s", KindOf(err))
		}
	})
}
