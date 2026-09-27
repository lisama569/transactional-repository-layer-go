package txrepo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	maxPageLimit  = 1000
	maxSessions   = 1024
	cursorVersion = "v1"
)

// Store 是本地存储引擎：内存数据 + 可选 JSON 快照持久化。
// 所有方法并发安全；写路径只由事务提交触发。
type Store struct {
	mu        sync.RWMutex
	manifests map[string]Manifest
	changes   []ChangeRecord
	seq       uint64            // 全局单调序号
	idem      map[string]string // 幂等键 -> 变更记录 ID
	sessions  map[string]snapshot
	path      string // 快照文件路径，空表示纯内存
}

// Open 打开（或创建）位于 path 的存储；path 为空时使用纯内存存储。
func Open(path string) (*Store, error) {
	s := &Store{
		manifests: map[string]Manifest{},
		idem:      map[string]string{},
		sessions:  map[string]snapshot{},
		path:      path,
	}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, &Error{Kind: KindInternal, Op: "Open", Msg: "read snapshot", Err: err}
	}
	var snap persistedState
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, &Error{Kind: KindInternal, Op: "Open", Msg: "decode snapshot", Err: err}
	}
	s.manifests = snap.Manifests
	s.changes = snap.Changes
	s.seq = snap.Seq
	s.idem = snap.Idem
	if s.manifests == nil {
		s.manifests = map[string]Manifest{}
	}
	if s.idem == nil {
		s.idem = map[string]string{}
	}
	return s, nil
}

// persistedState 快照文件格式。
type persistedState struct {
	Manifests map[string]Manifest `json:"manifests"`
	Changes   []ChangeRecord      `json:"changes"`
	Seq       uint64              `json:"seq"`
	Idem      map[string]string   `json:"idem"`
}

// snapshot 某时刻的一致性读视图，用于稳定分页。
type snapshot struct {
	manifests map[string]Manifest
	changes   []ChangeRecord
	seq       uint64
}

// readSnapshot 在持锁下拷贝当前状态。
func (s *Store) readSnapshot() snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ms := make(map[string]Manifest, len(s.manifests))
	for k, v := range s.manifests {
		ms[k] = v
	}
	cs := make([]ChangeRecord, len(s.changes))
	copy(cs, s.changes)
	return snapshot{manifests: ms, changes: cs, seq: s.seq}
}

// apply 以原子方式应用一批事务变更并落盘。
// 先整体校验（版本、存在性、幂等），全部通过后才写入，保证不留半成品。
func (s *Store) apply(ctx context.Context, batch txBatch) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Op: "Commit", Msg: "canceled before apply", Err: err}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) 幂等去重：已应用过的幂等键直接跳过，不产生重复变更记录。
	pending := make([]mutation, 0, len(batch.mutations))
	for _, m := range batch.mutations {
		if m.idempotencyKey != "" {
			if _, done := s.idem[m.idempotencyKey]; done {
				continue
			}
		}
		pending = append(pending, m)
	}

	// 2) 在工作副本上边校验边应用；任一步失败则整批丢弃，不留半成品。
	manifests := make(map[string]Manifest, len(s.manifests))
	for k, v := range s.manifests {
		manifests[k] = v
	}
	versions := make(map[string]uint64, len(s.manifests))
	for k, v := range s.manifests {
		versions[k] = v.Version
	}
	touched := map[string]bool{} // 本批次内已被修改过的清单
	changes := s.changes
	idem := make(map[string]string, len(s.idem))
	for k, v := range s.idem {
		idem[k] = v
	}
	seq := s.seq
	now := time.Now().UTC()

	for _, m := range pending {
		_, exists := manifests[m.manifestID]
		switch m.op {
		case ChangeOpCreate:
			if exists {
				return &Error{Kind: KindConflict, Op: "Commit", Msg: fmt.Sprintf("manifest %q already exists", m.manifestID), Err: ErrConflict}
			}
		case ChangeOpUpdate, ChangeOpDelete:
			if !exists {
				return &Error{Kind: KindNotFound, Op: "Commit", Msg: fmt.Sprintf("manifest %q not found", m.manifestID), Err: ErrNotFound}
			}
			// 本批次内已改过的清单，期望版本相对于批次内当前版本；
			// 否则相对于事务外可见的已提交版本。
			want := versions[m.manifestID]
			if touched[m.manifestID] {
				want--
			}
			if m.expectVersion != want {
				return &Error{Kind: KindConflict, Op: "Commit",
					Msg:  fmt.Sprintf("manifest %q version mismatch: expect %d, actual %d", m.manifestID, m.expectVersion, want),
					Err:  ErrConflict}
			}
		}

		seq++
		versions[m.manifestID]++
		touched[m.manifestID] = true
		rec := ChangeRecord{
			ID:             fmt.Sprintf("chg-%d", seq),
			ManifestID:     m.manifestID,
			Op:             m.op,
			Version:        versions[m.manifestID],
			IdempotencyKey: m.idempotencyKey,
			Detail:         m.detail,
			Seq:            seq,
			At:             now,
		}
		switch m.op {
		case ChangeOpCreate, ChangeOpUpdate:
			after := *m.after
			after.Version = versions[m.manifestID]
			after.Seq = seq
			after.UpdatedAt = now
			if m.op == ChangeOpCreate {
				after.CreatedAt = now
			} else {
				after.CreatedAt = manifests[m.manifestID].CreatedAt
			}
			manifests[after.ID] = after
		case ChangeOpDelete:
			delete(manifests, m.manifestID)
		}
		changes = append(changes, rec)
		if m.idempotencyKey != "" {
			idem[m.idempotencyKey] = rec.ID
		}
	}

	// 3) 先落盘，成功后才换入内存状态；落盘失败则内存保持原样。
	if err := s.persistStateLocked(persistedState{
		Manifests: manifests,
		Changes:   changes,
		Seq:       seq,
		Idem:      idem,
	}); err != nil {
		return &Error{Kind: KindInternal, Op: "Commit", Msg: "persist failed, commit aborted", Err: err}
	}
	s.manifests = manifests
	s.changes = changes
	s.seq = seq
	s.idem = idem
	return nil
}

// persistStateLocked 把给定状态写入快照文件（path 为空时跳过）。调用方须持写锁。
func (s *Store) persistStateLocked(state persistedState) error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// cursor 游标线格式（JSON 后 base64）。
type cursor struct {
	V       string `json:"v"`              // 版本
	T       string `json:"t"`              // "m"=清单分页（快照会话），"c"=变更记录分页
	Session string `json:"s,omitempty"`    // 快照会话 ID
	Key     string `json:"k,omitempty"`    // 清单分页：最后一条 ID
	Seq     uint64 `json:"seq,omitempty"`  // 变更记录分页：最后一条 Seq
}

func encodeCursor(c cursor) string {
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(s string) (cursor, error) {
	var c cursor
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, &Error{Kind: KindInvalidCursor, Op: "List", Msg: "cursor is not valid base64", Err: ErrInvalidCursor}
	}
	if err := json.Unmarshal(data, &c); err != nil || c.V != cursorVersion || (c.T != "m" && c.T != "c") {
		return c, &Error{Kind: KindInvalidCursor, Op: "List", Msg: "cursor payload malformed", Err: ErrInvalidCursor}
	}
	return c, nil
}

func validatePageRequest(req PageRequest) error {
	if req.Limit <= 0 || req.Limit > maxPageLimit {
		return &Error{Kind: KindInvalidArgument, Op: "List",
			Msg: fmt.Sprintf("limit must be in (0, %d], got %d", maxPageLimit, req.Limit), Err: ErrInvalidArgument}
	}
	return nil
}

// listManifests 基于快照会话做稳定分页：首页固定一份读视图，
// 翻页期间的新增/更新不会造成重复或遗漏。
func (s *Store) listManifests(ctx context.Context, f Filter, req PageRequest) (Page[Manifest], error) {
	if err := ctx.Err(); err != nil {
		return Page[Manifest]{}, &Error{Kind: KindCanceled, Op: "ListManifests", Msg: "canceled", Err: err}
	}
	if err := validatePageRequest(req); err != nil {
		return Page[Manifest]{}, err
	}

	var snap snapshot
	var afterKey string
	if req.Cursor == "" {
		snap = s.readSnapshot()
		s.mu.Lock()
		if len(s.sessions) >= maxSessions { // 简单上限：本地场景直接清空重建
			s.sessions = map[string]snapshot{}
		}
		s.seq++ // 复用全局序号生成会话 ID
		sessionID := fmt.Sprintf("pg-%d", s.seq)
		s.sessions[sessionID] = snap
		s.mu.Unlock()
		afterKey = ""
		req.Cursor = encodeCursor(cursor{V: cursorVersion, T: "m", Session: sessionID})
	} else {
		c, err := decodeCursor(req.Cursor)
		if err != nil {
			return Page[Manifest]{}, err
		}
		if c.T != "m" {
			return Page[Manifest]{}, &Error{Kind: KindInvalidCursor, Op: "ListManifests", Msg: "cursor type mismatch", Err: ErrInvalidCursor}
		}
		s.mu.RLock()
		var ok bool
		snap, ok = s.sessions[c.Session]
		s.mu.RUnlock()
		if !ok {
			return Page[Manifest]{}, &Error{Kind: KindInvalidCursor, Op: "ListManifests", Msg: "cursor session expired", Err: ErrInvalidCursor}
		}
		afterKey = c.Key
	}

	// 在快照内按键序做 keyset 分页。
	ids := make([]string, 0, len(snap.manifests))
	for id, m := range snap.manifests {
		if id > afterKey && f.Matches(m) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	page := Page[Manifest]{Items: []Manifest{}}
	if len(ids) > req.Limit {
		ids = ids[:req.Limit]
		// 还有后续数据，生成下一页游标。
	}
	for _, id := range ids {
		page.Items = append(page.Items, snap.manifests[id])
	}
	if len(ids) == req.Limit && len(ids) > 0 {
		// 判断是否真有更多：快照中是否还存在 key 更大的匹配项。
		last := ids[len(ids)-1]
		more := false
		for id, m := range snap.manifests {
			if id > last && f.Matches(m) {
				more = true
				break
			}
		}
		if more {
			c, _ := decodeCursor(req.Cursor)
			c.Key = last
			page.NextCursor = encodeCursor(c)
		}
	}
	if page.NextCursor == "" && req.Cursor != "" {
		// 尾页：清理会话。
		if c, err := decodeCursor(req.Cursor); err == nil {
			s.mu.Lock()
			delete(s.sessions, c.Session)
			s.mu.Unlock()
		}
	}
	return page, nil
}

// listChanges 按清单 ID 分页列出变更记录；变更记录只增不改，用 Seq 做 keyset 游标。
func (s *Store) listChanges(ctx context.Context, manifestID string, req PageRequest) (Page[ChangeRecord], error) {
	if err := ctx.Err(); err != nil {
		return Page[ChangeRecord]{}, &Error{Kind: KindCanceled, Op: "ListChanges", Msg: "canceled", Err: err}
	}
	if manifestID == "" {
		return Page[ChangeRecord]{}, &Error{Kind: KindInvalidArgument, Op: "ListChanges", Msg: "manifest id required", Err: ErrInvalidArgument}
	}
	if err := validatePageRequest(req); err != nil {
		return Page[ChangeRecord]{}, err
	}
	var afterSeq uint64
	if req.Cursor != "" {
		c, err := decodeCursor(req.Cursor)
		if err != nil {
			return Page[ChangeRecord]{}, err
		}
		if c.T != "c" {
			return Page[ChangeRecord]{}, &Error{Kind: KindInvalidCursor, Op: "ListChanges", Msg: "cursor type mismatch", Err: ErrInvalidCursor}
		}
		afterSeq = c.Seq
	}

	s.mu.RLock()
	matched := make([]ChangeRecord, 0, 8)
	for _, ch := range s.changes { // changes 按 Seq 升序追加
		if ch.ManifestID == manifestID && ch.Seq > afterSeq {
			matched = append(matched, ch)
		}
	}
	s.mu.RUnlock()

	page := Page[ChangeRecord]{Items: []ChangeRecord{}}
	if len(matched) > req.Limit {
		page.Items = matched[:req.Limit]
		page.NextCursor = encodeCursor(cursor{V: cursorVersion, T: "c", Seq: page.Items[len(page.Items)-1].Seq})
	} else {
		page.Items = matched
	}
	return page, nil
}

// getManifest 读取单个清单。
func (s *Store) getManifest(ctx context.Context, id string) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, &Error{Kind: KindCanceled, Op: "Get", Msg: "canceled", Err: err}
	}
	s.mu.RLock()
	m, ok := s.manifests[id]
	s.mu.RUnlock()
	if !ok {
		return Manifest{}, &Error{Kind: KindNotFound, Op: "Get", Msg: fmt.Sprintf("manifest %q not found", id), Err: ErrNotFound}
	}
	return m, nil
}
