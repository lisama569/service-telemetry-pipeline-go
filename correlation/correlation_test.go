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

func TestReleaseAllowsReinherit(t *testing.T) {
	tr := NewTracker(nil)
	id := "abcdef0123456789"
	_, first := tr.Ensure(context.Background(), id)
	t.Logf("first=%s 判定依据: 上游标识合法, 继承", first)
	tr.Release(first.RequestID)
	_, second := tr.Ensure(context.Background(), id)
	t.Logf("second=%s 判定依据: 前一请求已结束并归还标识, 可重新继承", second)
	if second.Source != SourceInherited || second.RequestID != id {
		t.Fatalf("归还后应能重新继承, 得到 %s", second)
	}
}

func TestInFlightDuplicateStillReplaced(t *testing.T) {
	tr := NewTracker(nil)
	id := "abcdef0123456789"
	_, first := tr.Ensure(context.Background(), id)
	_, dup := tr.Ensure(context.Background(), id)
	t.Logf("first=%s dup=%s 判定依据: 标识仍在处理中, 重复须替换", first, dup)
	if dup.Source != SourceReplacedDuplicate {
		t.Fatalf("处理中的重复标识应被替换, 得到 %s", dup)
	}
	if tr.Active() != 2 {
		t.Fatalf("两个活动标识都应登记, Active=%d", tr.Active())
	}
}

func TestActiveSetBounded(t *testing.T) {
	tr := NewTracker(nil, WithMaxActive(4))
	ids := []string{"aaaa0000aaaa0000", "bbbb0000bbbb0000", "cccc0000cccc0000",
		"dddd0000dddd0000", "eeee0000eeee0000"}
	for _, id := range ids {
		tr.Ensure(context.Background(), id)
	}
	if tr.Active() != 4 {
		t.Fatalf("判重记录应受容量上限约束, Active=%d", tr.Active())
	}
	// 最旧的 aaaa... 已被淘汰, 可重新继承; 最新的 eeee... 仍在处理中, 仍判重。
	_, re := tr.Ensure(context.Background(), ids[0])
	t.Logf("re=%s 判定依据: 最旧记录已被容量上限淘汰, 可重新继承", re)
	if re.Source != SourceInherited {
		t.Fatalf("被淘汰的标识应可重新继承, 得到 %s", re)
	}
	_, dup := tr.Ensure(context.Background(), ids[4])
	t.Logf("dup=%s 判定依据: 标识仍在处理中, 判重仍生效", dup)
	if dup.Source != SourceReplacedDuplicate {
		t.Fatalf("活动中的标识仍应判重, 得到 %s", dup)
	}
}

func TestDeterministicIDFunc(t *testing.T) {
	seq := 0
	tr := NewTracker(nil, WithIDFunc(func() string {
		seq++
		return fmt.Sprintf("%016x", seq)
	}))
	_, tc := tr.Ensure(context.Background(), "")
	t.Logf("correlation=%s 判定依据: 注入确定性 ID 生成器, 便于复现", tc)
	if tc.TraceID != "0000000000000001" || tc.RequestID != "0000000000000002" {
		t.Fatalf("确定性 ID 生成失败: %s", tc)
	}
}
