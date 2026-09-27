package datalayer

import (
	"encoding/base64"
	"encoding/json"
)

// cursor 是分页游标的内部表示：记录快照修订号与上一页最后一条记录的 ID，
// 保证翻页期间数据变化不会造成重复或遗漏。
type cursor struct {
	Revision int64  `json:"r"`
	LastID   string `json:"l"`
}

// encodeCursor 生成游标字符串（JSON + base64url，对外不透明）。
func encodeCursor(c cursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursor 解析游标；格式非法时返回 KindInvalidCursor 错误。
func decodeCursor(s string) (cursor, error) {
	const op = "cursor.decode"
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, newError(KindInvalidCursor, op, "malformed cursor encoding", err)
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return cursor{}, newError(KindInvalidCursor, op, "malformed cursor payload", err)
	}
	if c.Revision < 0 {
		return cursor{}, newError(KindInvalidCursor, op, "negative revision in cursor", nil)
	}
	return c, nil
}
