package datalayer

import "time"

// Checklist 清单记录。Version 用于乐观锁条件更新。
type Checklist struct {
	ID        string
	Title     string
	Owner     string
	Done      bool
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ChangeRecord 变更记录，每次成功写入产生一条；同一幂等键只产生一条。
type ChangeRecord struct {
	ID             string
	ChecklistID    string
	Op             string // create / update / delete
	IdempotencyKey string
	Revision       int64 // 存储全局修订号，用于分页快照
	CreatedAt      time.Time
}

// Filter 清单查询条件；零值字段不参与过滤。
type Filter struct {
	Owner string
	Done  *bool
}

// PageRequest 分页请求。Cursor 为空表示第一页。
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page 分页结果。NextCursor 为空表示没有下一页。
type Page[T any] struct {
	Items     []T
	NextCursor string
}
