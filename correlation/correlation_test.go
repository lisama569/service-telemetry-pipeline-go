package correlation

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestEnsureMissingUpstream(t *testing.T) {
	tr := NewTracker(nil)
	_, tc := tr.Ensure(context.Background(), "")
	t.Logf("correlation=%s 判定依据: 上游标识缺失, 本地生成", tc)
	if tc.Source != SourceGenerated {
		t.Fatalf("期望 SourceGenerated, 得到 %s", tc.Source)
	}
	if tc.RequestID == "" || tc.TraceID == "" {
		t.Fatal("标识不应为空")
	}
}

func TestEnsureInherited(t *testing.T) {
	tr := NewTracker(nil)
	_, tc := tr.Ensure(context.Background(), "abcdef0123456789")
	t.Logf("correlation=%s 判定依据: 上游标识合法, 原样继承", tc)
	if tc.Source != SourceInherited || tc.RequestID != "abcdef0123456789" {
		t.Fatalf("继承失败: %s", tc)
	}
}

func TestEnsureInvalidUpstream(t *testing.T) {
	tr := NewTracker(nil)
	for _, bad := range []string{"!!not-hex!!", "abc", ""} {
		ctx, tc := tr.Ensure(context.Background(), bad)
		t.Logf("correlation=%s 判定依据: 上游标识 %q 非法, 已替换", tc, bad)
		if bad != "" && tc.Source != SourceReplacedInvalid {
			t.Fatalf("上游 %q 期望 SourceReplacedInvalid, 得到 %s", bad, tc.Source)
		}
		if FromContextMust(ctx).TraceID == "" {
			t.Fatal("链路不得中断: TraceID 为空")
		}
	}
}

func TestEnsureDuplicateUpstream(t *testing.T) {
	tr := NewTracker(nil)
	id := "abcdef0123456789"
	_, first := tr.Ensure(context.Background(), id)
	_, second := tr.Ensure(context.Background(), id)
	t.Logf("first=%s second=%s 判定依据: 第二次上游标识与活动标识重复, 已替换", first, second)
	if second.Source != SourceReplacedDuplicate {
		t.Fatalf("期望 SourceReplacedDuplicate, 得到 %s", second.Source)
	}
	if second.RequestID == id {
		t.Fatal("重复标识必须被替换")
	}
	if first.TraceID == second.TraceID {
		t.Fatal("不同链路的 TraceID 不应相同")
	}
}

func TestEnsureExistingContextNotOverridden(t *testing.T) {
	tr := NewTracker(nil)
	ctx, tc := tr.Ensure(context.Background(), "")
	ctx2, tc2 := tr.Ensure(ctx, "0123456789abcdef")
	if tc != tc2 || ctx2 != ctx {
		t.Fatal("已有上下文不得被覆盖, 防止串号")
	}
}

func TestSpawnKeepsTraceAcrossConcurrency(t *testing.T) {
	tr := NewTracker(nil)
	ctx, root := tr.Ensure(context.Background(), "")
	const n = 64
	ids := make(chan Context, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, child := tr.Spawn(ctx)
			ids <- child
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for c := range ids {
		t.Logf("correlation=%s 判定依据: 派生子任务, TraceID 继承, RequestID 重生成", c)
		if c.TraceID != root.TraceID {
			t.Fatalf("并发任务 TraceID 串号: 期望 %s, 得到 %s", root.TraceID, c.TraceID)
		}
		if c.RequestID == root.RequestID || seen[c.RequestID] {
			t.Fatalf("RequestID 重复: %s", c.RequestID)
		}
		seen[c.RequestID] = true
	}
	if len(seen) != n {
		t.Fatalf("丢失子上下文: 期望 %d, 得到 %d", n, len(seen))
	}
}

func TestSpawnWithoutParent(t *testing.T) {
	tr := NewTracker(nil)
	_, tc := tr.Spawn(context.Background())
	t.Logf("correlation=%s 判定依据: 无父上下文, 等价于新生成", tc)
	if tc.Source != SourceGenerated || tc.TraceID == "" {
		t.Fatalf("无父上下文派生失败: %s", tc)
	}
}

// FromContextMust 测试辅助。
func FromContextMust(ctx context.Context) Context {
	tc, _ := FromContext(ctx)
	return tc
}

func TestIDSourceStringDistinct(t *testing.T) {
	sources := []IDSource{SourceGenerated, SourceInherited, SourceReplacedInvalid, SourceReplacedDuplicate}
	seen := map[string]bool{}
	for _, s := range sources {
		if seen[s.String()] {
			t.Fatalf("来源描述不可区分: %s", s)
		}
		seen[s.String()] = true
	}
	fmt.Println("来源可区分:", seen)
}
