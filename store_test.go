package txrepo

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func seedManifests(t *testing.T, repo *Repository, n int) {
	t.Helper()
	ctx := context.Background()
	err := repo.WithTx(ctx, func(tx *Tx) error {
		for i := 0; i < n; i++ {
			m := Manifest{ID: fmt.Sprintf("m-%02d", i), Name: fmt.Sprintf("item-%02d", i), Status: "open"}
			if err := tx.Create(ctx, m, ""); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Logf("state: seeded %d manifests", n)
}

func TestListManifestsPaginationBoundaries(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	seedManifests(t, repo, 7) // 7 条，limit=3 → 3/3/1

	var got []string
	cursor := ""
	for page := 0; ; page++ {
		p, err := repo.ListManifests(ctx, Filter{Status: "open"}, PageRequest{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v (kind=%s)", page, err, KindOf(err))
		}
		t.Logf("state: page=%d items=%d hasNext=%v", page, len(p.Items), p.NextCursor != "")
		for _, m := range p.Items {
			got = append(got, m.ID)
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
		if page > 10 {
			t.Fatal("分页未终止")
		}
	}
	if len(got) != 7 {
		t.Fatalf("want 7 items, got %d: %v", len(got), got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("重复条目 %s", id)
		}
		seen[id] = true
	}
}

func TestListManifestsStableUnderConcurrentWrites(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	seedManifests(t, repo, 5)

	t.Logf("input: 取第一页(limit=2)后，新增 3 条并更新已有条目，再继续翻页")
	p1, err := repo.ListManifests(ctx, Filter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	// 翻页期间发生写入。
	if err := repo.WithTx(ctx, func(tx *Tx) error {
		for i := 5; i < 8; i++ {
			if err := tx.Create(ctx, Manifest{ID: fmt.Sprintf("m-%02d", i), Name: "new"}, ""); err != nil {
				return err
			}
		}
		m, _ := tx.Get(ctx, "m-00")
		m.Status = "closed" // 让首条不再匹配空筛选之外的状态
		return tx.Update(ctx, m, m.Version, "", "close m-00")
	}); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, m := range p1.Items {
		got[m.ID] = true
	}
	cursor := p1.NextCursor
	for cursor != "" {
		p, err := repo.ListManifests(ctx, Filter{}, PageRequest{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("kind=%s err=%v", KindOf(err), err)
		}
		for _, m := range p.Items {
			if got[m.ID] {
				t.Fatalf("翻页期间出现重复条目 %s", m.ID)
			}
			got[m.ID] = true
		}
		cursor = p.NextCursor
	}
	t.Logf("state: 快照内共 %d 条（翻页期间新增的不应混入）", len(got))
	if len(got) != 5 {
		t.Fatalf("快照分页应恰好覆盖首页时的 5 条, got %d", len(got))
	}
}

func TestListInvalidCursorAndParams(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	seedManifests(t, repo, 3)

	cases := []struct {
		name string
		req  PageRequest
		want ErrorKind
	}{
		{"乱码游标", PageRequest{Limit: 2, Cursor: "!!!not-base64!!!"}, KindInvalidCursor},
		{"伪造游标", PageRequest{Limit: 2, Cursor: "eyJ2Ijoidjk5In0"}, KindInvalidCursor},
		{"limit为零", PageRequest{Limit: 0}, KindInvalidArgument},
		{"limit超限", PageRequest{Limit: 10000}, KindInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := repo.ListManifests(ctx, Filter{}, tc.req)
			t.Logf("input: %+v → failure kind=%s err=%v", tc.req, KindOf(err), err)
			if KindOf(err) != tc.want {
				t.Fatalf("want %s, got %s (%v)", tc.want, KindOf(err), err)
			}
		})
	}

	// 变更记录游标不能用于清单列表（类型不匹配）。
	pc, err := repo.ListChanges(ctx, "m-00", PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if pc.NextCursor != "" {
		_, err = repo.ListManifests(ctx, Filter{}, PageRequest{Limit: 2, Cursor: pc.NextCursor})
		t.Logf("input: 跨类型游标 → failure kind=%s", KindOf(err))
		if KindOf(err) != KindInvalidCursor {
			t.Fatalf("want invalid_cursor, got %s", KindOf(err))
		}
	}
}

func TestListChangesPagination(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	seedManifests(t, repo, 1)
	for i := 0; i < 4; i++ {
		if _, err := repo.UpdateWithRetry(ctx, "m-00", fmt.Sprintf("idem-u%d", i), 3, func(m Manifest) (Manifest, string, error) {
			m.Status = fmt.Sprintf("s%d", i)
			return m, fmt.Sprintf("update %d", i), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var total int
	cursor := ""
	for {
		p, err := repo.ListChanges(ctx, "m-00", PageRequest{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		total += len(p.Items)
		t.Logf("state: changes page items=%d ops=%v", len(p.Items), func() []ChangeOp {
			var ops []ChangeOp
			for _, c := range p.Items {
				ops = append(ops, c.Op)
			}
			return ops
		}())
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if total != 5 { // 1 create + 4 update
		t.Fatalf("want 5 change records, got %d", total)
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	ctx := context.Background()

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(store)
	if err := repo.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, "idem-p1")
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("state: 已提交 m1 并落盘 %s", path)

	// 重新打开，数据与幂等键都应恢复。
	store2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo2 := NewRepository(store2)
	m, err := repo2.Get(ctx, "m1")
	if err != nil {
		t.Fatalf("恢复后读取: %v", err)
	}
	t.Logf("state: 恢复 m1 version=%d", m.Version)

	// 相同幂等键重复执行，不应产生重复变更记录。
	if err := repo2.WithTx(ctx, func(tx *Tx) error {
		return tx.Create(ctx, Manifest{ID: "m1", Name: "alpha"}, "idem-p1")
	}); err != nil {
		t.Fatalf("幂等重放应成功: %v", err)
	}
	p, err := repo2.ListChanges(ctx, "m1", PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 {
		t.Fatalf("幂等重放后变更记录应仍为 1 条, got %d", len(p.Items))
	}
	t.Logf("state: 幂等重放后变更记录数=%d", len(p.Items))
}
