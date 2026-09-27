package txrepo

import (
	"strings"
	"time"
)

// ChangeOp 变更类型。
type ChangeOp string

const (
	ChangeOpCreate ChangeOp = "create"
	ChangeOpUpdate ChangeOp = "update"
	ChangeOpDelete ChangeOp = "delete"
)

// Manifest 清单记录。Version 用于乐观并发控制，Seq 为全局单调序号（分页锚点）。
type Manifest struct {
	ID         string
	Name       string
	Status     string
	Attributes map[string]string
	Version    uint64
	Seq        uint64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ChangeRecord 变更记录。IdempotencyKey 用于去重，保证重试不产生重复记录。
type ChangeRecord struct {
	ID             string
	ManifestID     string
	Op             ChangeOp
	Version        uint64 // 变更后 Manifest 的版本号
	IdempotencyKey string
	Detail         string
	Seq            uint64
	At             time.Time
}

// Filter 列表筛选条件，零值字段不参与筛选。
type Filter struct {
	Status     string
	NamePrefix string
}

// Matches 判断清单是否满足筛选条件。
func (f Filter) Matches(m Manifest) bool {
	if f.Status != "" && m.Status != f.Status {
		return false
	}
	if f.NamePrefix != "" && !strings.HasPrefix(m.Name, f.NamePrefix) {
		return false
	}
	return true
}

// PageRequest 分页请求。Cursor 为空表示第一页。
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page 一页结果。NextCursor 为空表示没有更多数据。
type Page[T any] struct {
	Items      []T
	NextCursor string
}
