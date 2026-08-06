package beam

import (
	"testing"
)

func TestStateCache_Empty(t *testing.T) {
	sc := newStateCache[int]()
	entry := sc.Get("key1", "win1")
	if entry.valid {
		t.Errorf("Get on empty cache: got valid=true, want false")
	}
	if entry.cleared {
		t.Errorf("Get on empty cache: got cleared=true, want false")
	}
	if entry.loaded {
		t.Errorf("Get on empty cache: got loaded=true, want false")
	}
	if got, want := entry.fresh, 0; got != want {
		t.Errorf("Get on empty cache: got fresh=%v, want %v", got, want)
	}
}

func TestStateCache_PutAndGet(t *testing.T) {
	sc := newStateCache[string]()
	sc.Put("k1", "w1", stateCacheEntry[string]{
		fresh:  "value1",
		runner: "runner1",
		loaded: true,
	})

	entry := sc.Get("k1", "w1")
	if !entry.valid {
		t.Errorf("Get after Put: got valid=false, want true")
	}
	if !entry.loaded {
		t.Errorf("Get after Put: got loaded=false, want true")
	}
	if got, want := entry.fresh, "value1"; got != want {
		t.Errorf("Get after Put: got fresh=%v, want %v", got, want)
	}
	if got, want := entry.runner, "runner1"; got != want {
		t.Errorf("Get after Put: got runner=%v, want %v", got, want)
	}
}

func TestStateCache_Clear(t *testing.T) {
	sc := newStateCache[int]()
	sc.Put("k1", "w1", stateCacheEntry[int]{fresh: 42})
	sc.Clear("k1", "w1")

	entry := sc.Get("k1", "w1")
	if entry.valid {
		t.Errorf("Get after Clear: got valid=true, want false")
	}
	if !entry.cleared {
		t.Errorf("Get after Clear: got cleared=false, want true")
	}
	if got, want := entry.fresh, 0; got != want {
		t.Errorf("Get after Clear: got fresh=%v, want %v", got, want)
	}
}

func TestStateCache_KeyWindowIsolation(t *testing.T) {
	sc := newStateCache[int]()
	sc.Put("k1", "w1", stateCacheEntry[int]{fresh: 10})
	sc.Put("k1", "w2", stateCacheEntry[int]{fresh: 20})
	sc.Put("k2", "w1", stateCacheEntry[int]{fresh: 30})

	if got, want := sc.Get("k1", "w1").fresh, 10; got != want {
		t.Errorf("k1/w1: got %v, want %v", got, want)
	}
	if got, want := sc.Get("k1", "w2").fresh, 20; got != want {
		t.Errorf("k1/w2: got %v, want %v", got, want)
	}
	if got, want := sc.Get("k2", "w1").fresh, 30; got != want {
		t.Errorf("k2/w1: got %v, want %v", got, want)
	}
}

func TestStateCache_IterFlushesCache(t *testing.T) {
	sc := newStateCache[int]()
	sc.Put("k1", "w1", stateCacheEntry[int]{fresh: 100})
	sc.Put("k2", "w2", stateCacheEntry[int]{fresh: 200})

	count := 0
	found := map[string]int{}
	for key, entry := range sc.Iter() {
		count++
		found[key.k] = entry.fresh
	}

	if got, want := count, 2; got != want {
		t.Errorf("Iter count: got %v, want %v", got, want)
	}
	if got, want := found["k1"], 100; got != want {
		t.Errorf("found[k1]: got %v, want %v", got, want)
	}
	if got, want := found["k2"], 200; got != want {
		t.Errorf("found[k2]: got %v, want %v", got, want)
	}

	// Verify cache is flushed after Iter()
	postEntry := sc.Get("k1", "w1")
	if postEntry.valid {
		t.Errorf("Get after Iter: got valid=true, want false (cache should be flushed)")
	}
}
