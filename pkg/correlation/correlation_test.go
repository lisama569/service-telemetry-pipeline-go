package correlation

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestResolveMissingGeneratesID(t *testing.T) {
	store := NewStore()
	c, replaced := Resolve(context.Background(), store, "")
	if !replaced {
		t.Fatal("missing upstream id should report replaced=true")
	}
	if c.Origin() != OriginGenerated {
		t.Fatalf("origin = %s, want generated", c.Origin())
	}
	if !ValidID(c.CorrelationID()) || c.CorrelationID() == "" {
		t.Fatalf("generated correlation id invalid: %q", c.CorrelationID())
	}
	if c.TraceID() == "" || c.SpanID() == "" {
		t.Fatal("generated context must carry trace/span ids")
	}
	t.Logf("关联标识[缺失] -> 本地生成 corr=%s trace=%s span=%s 判定依据=%s",
		c.CorrelationID(), c.TraceID(), c.SpanID(), c.Origin())
}

func TestResolveInherited(t *testing.T) {
	store := NewStore()
	c, replaced := Resolve(context.Background(), store, "upstream-abc_123")
	if replaced || c.Origin() != OriginInherited || c.CorrelationID() != "upstream-abc_123" {
		t.Fatalf("got origin=%s id=%s replaced=%v", c.Origin(), c.CorrelationID(), replaced)
	}
	t.Logf("关联标识[正常] -> 继承 corr=%s 判定依据=%s", c.CorrelationID(), c.Origin())
}

func TestResolveMalformedReplaced(t *testing.T) {
	store := NewStore()
	for _, bad := range []string{"bad id with space", "中文标识", strings.Repeat("a", 65), "a/b", ""} {
		c, replaced := Resolve(context.Background(), store, bad)
		if bad != "" && !replaced {
			t.Fatalf("malformed %q should be replaced", bad)
		}
		if bad != "" && c.Origin() != OriginMalformed {
			t.Fatalf("origin for %q = %s, want malformed", bad, c.Origin())
		}
		if !ValidID(c.CorrelationID()) {
			t.Fatalf("replacement id invalid for %q", bad)
		}
		t.Logf("关联标识[畸形 upstream=%q] -> 替换为 corr=%s 判定依据=%s",
			bad, c.CorrelationID(), c.Origin())
	}
}

func TestResolveDuplicateMarked(t *testing.T) {
	store := NewStore()
	first, _ := Resolve(context.Background(), store, "dup-id")
	second, _ := Resolve(context.Background(), store, "dup-id")
	if first.Origin() != OriginInherited || second.Origin() != OriginDuplicate {
		t.Fatalf("origins = %s,%s want inherited,duplicate", first.Origin(), second.Origin())
	}
	if first.CorrelationID() != second.CorrelationID() {
		t.Fatal("duplicate id must be preserved, not replaced")
	}
	t.Logf("关联标识[重复] -> 沿用 corr=%s 首次=%s 再次=%s 判定依据=%s",
		second.CorrelationID(), first.Origin(), second.Origin(), second.Origin())
}

func TestChildSpanHierarchy(t *testing.T) {
	store := NewStore()
	root, _ := Resolve(context.Background(), store, "root-id")
	child := ChildSpan(root)
	if child.TraceID() != root.TraceID() || child.CorrelationID() != root.CorrelationID() {
		t.Fatal("child must stay in same trace/correlation")
	}
	if child.ParentSpanID() != root.SpanID() || child.SpanID() == root.SpanID() {
		t.Fatal("child span parent link broken or span id not unique")
	}
	t.Logf("关联标识[嵌套] corr=%s parentSpan=%s -> childSpan=%s",
		child.CorrelationID(), root.SpanID(), child.SpanID())
}

func TestConcurrentForkNoCrossTalk(t *testing.T) {
	store := NewStore()
	root, _ := Resolve(context.Background(), store, "concurrent-id")

	const n = 200
	var wg sync.WaitGroup
	spanIDs := make([][]string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			c := Fork(root)
			ids := []string{c.CorrelationID(), c.TraceID(), c.SpanID(), c.ParentSpanID()}
			// 后台任务再次嵌套派生也不能影响其他 goroutine。
			c2 := ChildSpan(c)
			ids = append(ids, c2.SpanID())
			spanIDs[i] = ids
		}(i)
	}
	wg.Wait()

	seenSpans := make(map[string]int)
	for _, ids := range spanIDs {
		if ids[0] != "concurrent-id" || ids[1] != root.TraceID() {
			t.Fatalf("fork lost correlation/trace: %v", ids)
		}
		if ids[2] == root.SpanID() {
			t.Fatal("fork must create a new span")
		}
		seenSpans[ids[2]]++
		seenSpans[ids[4]]++
	}
	for sid, n2 := range seenSpans {
		if n2 != 1 {
			t.Fatalf("span id %s observed %d times, goroutine cross-talk suspected", sid, n2)
		}
	}
	t.Logf("关联标识[并发] %d 个 goroutine Fork 全部保持 corr=%s trace=%s 且 span 无串号",
		n, root.CorrelationID(), root.TraceID())
}

func TestContextPropagation(t *testing.T) {
	store := NewStore()
	c, _ := Resolve(context.Background(), store, "ctx-id")
	ctx := IntoContext(context.Background(), c)
	got, ok := FromContext(ctx)
	if !ok || got.CorrelationID() != "ctx-id" {
		t.Fatal("context round-trip failed")
	}
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("empty context should report missing")
	}
}
