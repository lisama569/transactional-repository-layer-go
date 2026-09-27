// Package txrepo 提供围绕清单（Manifest）与变更记录（ChangeRecord）的
// 本地数据访问层：事务组合、游标分页、条件更新与错误分类。
package txrepo

import (
	"context"
	"errors"
	"fmt"
)

// ErrorKind 对错误进行分类，便于调用方按类别处理与重试。
type ErrorKind string

const (
	KindInvalidArgument ErrorKind = "invalid_argument" // 参数错误
	KindInvalidCursor   ErrorKind = "invalid_cursor"   // 游标非法
	KindNotFound        ErrorKind = "not_found"        // 目标不存在
	KindConflict        ErrorKind = "conflict"         // 版本冲突 / 并发更新
	KindCanceled        ErrorKind = "canceled"         // 调用被取消
	KindTxClosed        ErrorKind = "tx_closed"        // 事务已结束仍被使用
	KindInternal        ErrorKind = "internal"         // 内部错误（含提交失败）
)

// 哨兵错误，可用 errors.Is 判定。
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrInvalidCursor   = errors.New("invalid cursor")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("version conflict")
	ErrCanceled        = errors.New("operation canceled")
	ErrTxClosed        = errors.New("transaction already closed")
)

// Error 携带分类信息的错误类型。
type Error struct {
	Kind ErrorKind
	Op   string // 出错的操作名
	Msg  string
	Err  error // 底层原因
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Op, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Msg)
}

func (e *Error) Unwrap() error { return e.Err }

// Is 支持用哨兵错误判定：errors.Is(err, ErrConflict)。
func (e *Error) Is(target error) bool {
	switch e.Kind {
	case KindInvalidArgument:
		return target == ErrInvalidArgument
	case KindInvalidCursor:
		return target == ErrInvalidCursor
	case KindNotFound:
		return target == ErrNotFound
	case KindConflict:
		return target == ErrConflict
	case KindCanceled:
		return target == ErrCanceled
	case KindTxClosed:
		return target == ErrTxClosed
	}
	return false
}

// KindOf 返回 err 的分类；无法识别时返回 KindInternal。
func KindOf(err error) ErrorKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return KindInvalidArgument
	case errors.Is(err, ErrInvalidCursor):
		return KindInvalidCursor
	case errors.Is(err, ErrNotFound):
		return KindNotFound
	case errors.Is(err, ErrConflict):
		return KindConflict
	case errors.Is(err, ErrCanceled):
		return KindCanceled
	case errors.Is(err, ErrTxClosed):
		return KindTxClosed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return KindCanceled
	default:
		return KindInternal
	}
}
