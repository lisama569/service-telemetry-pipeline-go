package pipeline

import (
	"sync"
	"time"
)

// fakeClock 是测试用可控时钟：每次读取前进 1ms，jump 可大步拨快。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1000, 0)} }

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(time.Millisecond)
	return f.t
}

func (f *fakeClock) jump(d time.Duration) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
	return f.t
}

func (f *fakeClock) current() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}
