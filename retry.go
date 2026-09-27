package datalayer

import "context"

// RetryOptions 控制冲突重试行为。
type RetryOptions struct {
	MaxAttempts int // 含首次尝试；<=0 时使用默认值 3
}

const defaultMaxAttempts = 3

// WithRetry 执行 op，遇到 KindConflict 错误时重试（op 内部应重新读取
// 最新版本，或借助幂等键保证重复执行无副作用），直到成功、出现非冲突
// 错误或达到最大尝试次数。ctx 取消时立即返回 KindCanceled。
func WithRetry(ctx context.Context, opts RetryOptions, op func(ctx context.Context) error) error {
	const opName = "retry"
	attempts := opts.MaxAttempts
	if attempts <= 0 {
		attempts = defaultMaxAttempts
	}
	var err error
	for i := 0; i < attempts; i++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return canceledError(opName, ctx)
		}
		err = op(ctx)
		if err == nil || !IsKind(err, KindConflict) {
			return err
		}
	}
	return err
}
