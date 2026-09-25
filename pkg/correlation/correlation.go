// Package correlation 提供请求关联标识与追踪上下文的生成、继承与传播。
//
// 设计要点：
//   - 上游标识缺失或格式非法时本地生成新标识，绝不返回空标识、绝不中断链路；
//   - 上游在本进程内重复出现的标识被保留沿用，但以 OriginDuplicate 标记；
//   - Context 为不可变值拷贝，Fork/ChildSpan 均返回新副本，
//     因此可安全地跨 goroutine 与后台任务传播，不存在共享可变状态导致的串号。
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"sync"
)

// Origin 描述关联标识的来源，用于区分上游标识缺失/畸形/重复等情况。
type Origin int

const (
	// OriginGenerated 上游未提供标识，本地新生成。
	OriginGenerated Origin = iota
	// OriginInherited 正常继承上游标识。
	OriginInherited
	// OriginMalformed 上游标识格式非法，已替换为本地标识。
	OriginMalformed
	// OriginDuplicate 上游标识在本进程内重复出现，沿用首个并标记。
	OriginDuplicate
)

func (o Origin) String() string {
	switch o {
	case OriginGenerated:
		return "generated"
	case OriginInherited:
		return "inherited"
	case OriginMalformed:
		return "malformed"
	case OriginDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// Context 携带关联标识与追踪上下文，是只读快照（值类型，可安全拷贝）。
type Context struct {
	correlationID string
	traceID       string
	spanID        string
	parentSpanID  string
	origin        Origin
}

func (c Context) CorrelationID() string { return c.correlationID }
func (c Context) TraceID() string       { return c.traceID }
func (c Context) SpanID() string        { return c.spanID }
func (c Context) ParentSpanID() string  { return c.parentSpanID }
func (c Context) Origin() Origin        { return c.origin }

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidID 校验关联标识格式：1~64 位字母、数字、'-' 或 '_'。
func ValidID(id string) bool { return idPattern.MatchString(id) }

// NewID 生成 32 字符的十六进制随机标识。
func NewID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Store 记录本进程已见过的上游关联标识，用于检测重复。并发安全。
type Store struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

func NewStore() *Store {
	return &Store{seen: make(map[string]struct{})}
}

// Observe 原子地登记一个上游标识，返回它此前是否已出现过。
func (s *Store) Observe(upstreamID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[upstreamID]; ok {
		return true
	}
	s.seen[upstreamID] = struct{}{}
	return false
}

// newContext 构造一份完整的关联/追踪上下文。
func newContext(corrID, traceID, parentSpan string, origin Origin) Context {
	return Context{
		correlationID: corrID,
		traceID:       traceID,
		spanID:        newSpanID(),
		parentSpanID:  parentSpan,
		origin:        origin,
	}
}

// Resolve 根据上游标识解析出可用的关联上下文。
// 第二个返回值 replaced 为 true 表示上游标识缺失或非法、被本地生成值替换。
// 任何异常输入都不会产生空标识，也不会 panic，链路始终可用。
func Resolve(ctx context.Context, store *Store, upstreamID string) (Context, bool) {
	if upstreamID == "" {
		return newContext(NewID(), NewID(), "", OriginGenerated), true
	}
	if !ValidID(upstreamID) {
		// 非法标识绝不向下游透传：替换为本地标识，来源标记为 malformed。
		return newContext(NewID(), NewID(), "", OriginMalformed), true
	}
	origin := OriginInherited
	if store != nil && store.Observe(upstreamID) {
		origin = OriginDuplicate
	}
	// 关联标识沿用上游值；traceID 本地生成，保证本进程追踪可独立成段。
	return newContext(upstreamID, NewID(), "", origin), false
}

// ChildSpan 在父上下文上派生同步嵌套调用的新 span（同一 trace/关联标识）。
func ChildSpan(parent Context) Context {
	c := parent
	c.parentSpanID = parent.spanID
	c.spanID = newSpanID()
	return c
}

// Fork 派生可跨 goroutine / 后台任务安全传播的上下文副本。
// 副本共享 traceID 与 correlationID（保证可关联），但拥有独立 span，
// 多个并发 Fork 之间互不影响，不会串号。
func Fork(parent Context) Context {
	return ChildSpan(parent)
}

type ctxKey struct{}

// IntoContext 把关联上下文挂载到标准 context 上传播。
func IntoContext(ctx context.Context, c Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// FromContext 取出关联上下文；不存在时第二个返回值为 false。
func FromContext(ctx context.Context) (Context, bool) {
	c, ok := ctx.Value(ctxKey{}).(Context)
	return c, ok
}
