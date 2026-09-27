package datalayer

import (
	"context"
	"fmt"
	"testing"
)

func seed(t *testing.T, s *Store, n int) {
	t.Helper()
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		for i := 1; i <= n; i++ {
			id := fmt.Sprintf("item-%02d", i)
			item := Checklist{ID: id, Title: id, Owner: "alice", Done: i%2 == 0}
			if _, err := r.Checklists.Create(ctx, tx, item, ""); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Logf("seeded %d items, revision=%d", n, s.Revision())
}

func listPage(t *testing.T, s *Store, filter Filter, req PageRequest) Page[Checklist] {
	t.Helper()
	var out Page[Checklist]
	err := s.WithTx(context.Background(), func(tx *Tx, r Repositories) error {
		var err error
		out, err = r.Checklists.List(context.Background(), tx, filter, req)
		return err
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return out
}

func TestListPaginationBoundaries(t *testing.T) {
	s := NewStore()
	seed(t, s, 5)

	// 第一页：limit=2
	p1 := listPage(t, s, Filter{}, PageRequest{Limit: 2})
	t.Logf("page1: items=%v next=%q", ids(p1.Items), p1.NextCursor)
	if len(p1.Items) != 2 || p1.Items[0].ID != "item-01" || p1.NextCursor == "" {
		t.Fatalf("bad page1: %+v", p1)
	}

	// 翻到底：5 条 / 每页 2 条 = 3 页，最后一页 1 条且无下一页游标。
	seen := map[string]int{}
	cursor := ""
	pages := 0
	for {
		p := listPage(t, s, Filter{}, PageRequest{Limit: 2, Cursor: cursor})
		pages++
		for _, it := range p.Items {
			seen[it.ID]++
		}
		t.Logf("page%d: items=%v next=%q", pages, ids(p.Items), p.NextCursor)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if pages != 3 || len(seen) != 5 {
		t.Fatalf("pages=%d seen=%v", pages, seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("item %s seen %d times (duplicate across pages)", id, n)
		}
	}
	t.Logf("total pages=%d, all 5 items seen exactly once", pages)
}

func TestListStableDuringConcurrentWrites(t *testing.T) {
	s := NewStore()
	seed(t, s, 4)
	ctx := context.Background()

	p1 := listPage(t, s, Filter{}, PageRequest{Limit: 2})
	t.Logf("page1: items=%v cursor=%q", ids(p1.Items), p1.NextCursor)

	// 翻页期间：新增一条、更新一条已在第一页出现过的记录。
	err := s.WithTx(ctx, func(tx *Tx, r Repositories) error {
		if _, err := r.Checklists.Create(ctx, tx, Checklist{ID: "item-00", Title: "new"}, ""); err != nil {
			return err
		}
		it, err := r.Checklists.Get(ctx, tx, "item-01")
		if err != nil {
			return err
		}
		it.Title = "updated"
		_, err = r.Checklists.Update(ctx, tx, it, it.Version, "")
		return err
	})
	if err != nil {
		t.Fatalf("concurrent write: %v", err)
	}
	t.Logf("during paging: created item-00, updated item-01, revision=%d", s.Revision())

	// 第二页仍基于第一页的快照：不重复、不漏掉翻页前已满足条件的数据。
	p2 := listPage(t, s, Filter{}, PageRequest{Limit: 2, Cursor: p1.NextCursor})
	t.Logf("page2: items=%v next=%q", ids(p2.Items), p2.NextCursor)
	got := append(ids(p1.Items), ids(p2.Items)...)
	want := []string{"item-01", "item-02", "item-03", "item-04"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("snapshot paging mismatch: got %v want %v", got, want)
	}
}

func TestListFilter(t *testing.T) {
	s := NewStore()
	seed(t, s, 5) // item-02、item-04 Done=true

	done := true
	p := listPage(t, s, Filter{Owner: "alice", Done: &done}, PageRequest{Limit: 10})
	t.Logf("filtered(done=true): items=%v", ids(p.Items))
	if fmt.Sprint(ids(p.Items)) != "[item-02 item-04]" {
		t.Fatalf("bad filter result: %+v", p)
	}
}

func TestListInvalidCursor(t *testing.T) {
	s := NewStore()
	seed(t, s, 1)

	err := s.WithTx(context.Background(), func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.List(context.Background(), tx, Filter{}, PageRequest{Limit: 1, Cursor: "!!!not-a-cursor"})
		return err
	})
	if !IsKind(err, KindInvalidCursor) {
		t.Fatalf("want invalid_cursor, got %v (kind=%s)", err, KindOf(err))
	}
	t.Logf("bad cursor classified as kind=%s", KindOf(err))
}

func TestListInvalidLimit(t *testing.T) {
	s := NewStore()
	err := s.WithTx(context.Background(), func(tx *Tx, r Repositories) error {
		_, err := r.Checklists.List(context.Background(), tx, Filter{}, PageRequest{Limit: -1})
		return err
	})
	if !IsKind(err, KindInvalidArgument) {
		t.Fatalf("want invalid_argument, got %v (kind=%s)", err, KindOf(err))
	}
	t.Logf("bad limit classified as kind=%s (与游标错误区分开)", KindOf(err))
}

func ids(items []Checklist) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	return out
}
