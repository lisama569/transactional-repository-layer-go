// Package datalayer 提供本地数据访问层：清单与变更记录的持久化、
// 事务组合、游标分页查询与并发更新。
package datalayer

import (
	"context"
	"errors"
	"fmt"
)

// ErrorKind 对错误进行分类，便于调用方分别处理。
type ErrorKind string

const (
	// KindNotFound 目标记录不存在。
	KindNotFound ErrorKind = "not_found"
	// KindConflict 版本冲突（乐观锁失败），通常可重试。
	KindConflict ErrorKind = "conflict"
	// KindInvalidArgument 参数非法（如页大小、必填字段）。
	KindInvalidArgument ErrorKind = "invalid_argument"
	// KindInvalidCursor 游标非法或已过期，与参数错误区分开。
	KindInvalidCursor ErrorKind = "invalid_cursor"
	// KindCanceled 调用被取消（context 取消或超时）。
	KindCanceled ErrorKind = "canceled"
	// KindInternal 存储内部错误。
	KindInternal ErrorKind = "internal"
)

// Error 是数据访问层统一错误类型，携带分类与底层原因。
type Error struct {
	Kind ErrorKind
	Op   string // 出错的操作名
	Msg  string
	Err  error // 底层原因，可为 nil
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %s: %v", e.Op, e.Kind, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s: %s", e.Op, e.Kind, e.Msg)
}

func (e *Error) Unwrap() error { return e.Err }

func newError(kind ErrorKind, op, msg string, err error) *Error {
	return &Error{Kind: kind, Op: op, Msg: msg, Err: err}
}

// canceledError 把 context 取消/超时包装为 KindCanceled。
func canceledError(op string, ctx context.Context) *Error {
	return newError(KindCanceled, op, "operation canceled", ctx.Err())
}

// KindOf 从 error 链上提取分类；无法识别时返回 KindInternal。
// context 的取消/超时错误会被识别为 KindCanceled。
func KindOf(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return KindCanceled
	}
	return KindInternal
}

// IsKind 判断 err 是否属于指定分类。
func IsKind(err error, kind ErrorKind) bool { return KindOf(err) == kind }
