// Package correlation 提供请求关联标识与追踪上下文的生成、继承与传播。
//
// 传播规则：
//   - 上游标识缺失：本地生成新 RequestID，Source=SourceGenerated；
//   - 上游标识合法且与当前活动标识不重复：原样继承，Source=SourceInherited；
//   - 上游标识格式非法：丢弃并生成新标识，Source=SourceReplacedInvalid；
//   - 上游标识与当前活动标识重复：生成新标识，Source=SourceReplacedDuplicate；
//   - 以上任何情况都不中断链路，TraceID 在整条链路（含并发与后台任务）中保持不变。
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

// IDSource 描述关联标识的来源，用于区分各类上游标识情况。
type IDSource int

const (
	// SourceGenerated 表示上游未提供标识，本地新生成。
	SourceGenerated IDSource = iota
	// SourceInherited 表示上游标识合法，被原样继承。
	SourceInherited
	// SourceReplacedInvalid 表示上游标识格式非法，已替换为新标识。
	SourceReplacedInvalid
	// SourceReplacedDuplicate 表示上游标识与当前活动标识重复，已替换。
	SourceReplacedDuplicate
)

// String 返回可读的来源描述，便于日志打印判定依据。
func (s IDSource) String() string {
	switch s {
	case SourceGenerated:
		return "generated"
	case SourceInherited:
		return "inherited"
	case SourceReplacedInvalid:
		return "replaced_invalid"
	case SourceReplacedDuplicate:
		return "replaced_duplicate"
	default:
		return "unknown"
	}
}

// Context 是一次请求链路的追踪上下文。
type Context struct {
	TraceID   string
	RequestID string
	Source    IDSource
}

// String 便于在日志中打印关联标识与判定依据。
func (c Context) String() string {
	return fmt.Sprintf("trace=%s request=%s source=%s", c.TraceID, c.RequestID, c.Source)
}

// Validator 校验上游标识格式是否合法。
type Validator func(id string) bool

// DefaultValidator 默认校验：8~64 位十六进制字符串。
func DefaultValidator(id string) bool {
	if len(id) < 8 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		isDigit := r >= '0' && r <= '9'
		isLower := r >= 'a' && r <= 'f'
		isUpper := r >= 'A' && r <= 'F'
		if !isDigit && !isLower && !isUpper {
			return false
		}
	}
	return true
}

// DefaultMaxActive 判重记录的默认容量上限。
//
// 判重只在"当前仍在处理中"的请求之间进行：请求结束（正常或出错）调用
// Release 后标识即归还，可被后续请求重新继承。因此该记录只随并发在途
// 请求数增长，不随历史请求增长；容量上限是兜底：在途数异常超过上限时
// 按登记先后（FIFO）淘汰最旧条目，防止内存无界增长。
const DefaultMaxActive = 4096

// Tracker 负责标识的生成、继承与传播。
type Tracker struct {
	validate  Validator
	maxActive int
	mu        sync.Mutex
	active    map[string]struct{} // 当前进程内活动的 RequestID，用于重复检测
	order     []string            // 登记顺序（FIFO），用于容量淘汰；已 Release 的条目惰性跳过
}

// NewTracker 创建 Tracker；v 为 nil 时使用 DefaultValidator，
// 判重记录容量上限为 DefaultMaxActive。
func NewTracker(v Validator) *Tracker {
	return NewTrackerWithCapacity(v, DefaultMaxActive)
}

// NewTrackerWithCapacity 创建指定判重容量上限的 Tracker；maxActive<=0 时使用默认值。
func NewTrackerWithCapacity(v Validator, maxActive int) *Tracker {
	if v == nil {
		v = DefaultValidator
	}
	if maxActive <= 0 {
		maxActive = DefaultMaxActive
	}
	return &Tracker{validate: v, maxActive: maxActive, active: make(map[string]struct{})}
}

// newID 生成 32 位十六进制随机标识。
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("correlation: 生成随机标识失败: %v", err))
	}
	return hex.EncodeToString(b[:])
}

type ctxKey struct{}

// FromContext 从 context 中取出追踪上下文。
func FromContext(ctx context.Context) (Context, bool) {
	tc, ok := ctx.Value(ctxKey{}).(Context)
	return tc, ok
}

// IntoContext 将追踪上下文放入 context。
func IntoContext(ctx context.Context, tc Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, tc)
}

// Ensure 确保 ctx 中存在合法追踪上下文。
//
// 若 ctx 中已有上下文，直接沿用（不串号）；否则根据 upstreamID 的情况
// 生成新上下文，任何情况下都返回可用上下文，不中断链路。
func (t *Tracker) Ensure(ctx context.Context, upstreamID string) (context.Context, Context) {
	if tc, ok := FromContext(ctx); ok {
		return ctx, tc
	}
	tc := Context{TraceID: newID()}
	switch {
	case upstreamID == "":
		tc.RequestID = t.mint()
		tc.Source = SourceGenerated
	case !t.validate(upstreamID):
		tc.RequestID = t.mint()
		tc.Source = SourceReplacedInvalid
	case !t.tryRegister(upstreamID):
		// 已被当前进程内其他活动任务占用，视为重复。
		tc.RequestID = t.mint()
		tc.Source = SourceReplacedDuplicate
	default:
		tc.RequestID = upstreamID
		tc.Source = SourceInherited
	}
	return IntoContext(ctx, tc), tc
}

// Spawn 从已有上下文派生并发任务/后台任务的子上下文：
// TraceID 保持不变，RequestID 重新生成以避免与父任务串号。
// 若 ctx 中无上下文，则等同于 Ensure(ctx, "")。
func (t *Tracker) Spawn(ctx context.Context) (context.Context, Context) {
	parent, ok := FromContext(ctx)
	if !ok {
		return t.Ensure(ctx, "")
	}
	child := Context{
		TraceID:   parent.TraceID,
		RequestID: t.mint(),
		Source:    parent.Source,
	}
	return IntoContext(ctx, child), child
}

// Release 释放一个活动标识，任务结束（无论正常或出错）后调用：
// 标识归还可被后续请求重新继承，判重记录也随之收缩。
func (t *Tracker) Release(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.active, requestID)
}

// Active 返回当前仍在处理中的标识数量（判重记录大小），用于观测与测试。
func (t *Tracker) Active() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active)
}

// registerLocked 登记标识，必要时按 FIFO 淘汰最旧条目以维持容量上限。
// 调用方须持有锁。
func (t *Tracker) registerLocked(id string) {
	t.active[id] = struct{}{}
	t.order = append(t.order, id)
	for len(t.active) > t.maxActive && len(t.order) > 0 {
		oldest := t.order[0]
		t.order = t.order[1:]
		// 已 Release 的条目不在 active 中，惰性跳过。
		if _, ok := t.active[oldest]; ok {
			delete(t.active, oldest)
		}
	}
	// order 中积压的已释放条目定期清理，避免切片本身无界增长。
	if len(t.order) > 4*t.maxActive {
		kept := t.order[:0]
		for _, id := range t.order {
			if _, ok := t.active[id]; ok {
				kept = append(kept, id)
			}
		}
		t.order = kept
	}
}

// mint 生成并登记一个不重复的新标识。
func (t *Tracker) mint() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		id := newID()
		if _, dup := t.active[id]; !dup {
			t.registerLocked(id)
			return id
		}
	}
}

// tryRegister 原子地检查并登记标识；返回 false 表示已存在（重复）。
func (t *Tracker) tryRegister(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, dup := t.active[id]; dup {
		return false
	}
	t.registerLocked(id)
	return true
}
