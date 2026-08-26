// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beam

import (
	"reflect"
	"testing"
	"time"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
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

	tests := []struct {
		name string
		key  string
		win  string
		want int
	}{
		{name: "k1_w1", key: "k1", win: "w1", want: 10},
		{name: "k1_w2", key: "k1", win: "w2", want: 20},
		{name: "k2_w1", key: "k2", win: "w1", want: 30},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sc.Get(tc.key, tc.win).fresh; got != tc.want {
				t.Errorf("Get(%q, %q).fresh = %v, want %v", tc.key, tc.win, got, tc.want)
			}
		})
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

func TestStateBase(t *testing.T) {
	var s state
	s.isState()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected persist on uninitialized base state to panic")
		}
	}()
	_ = s.persist()
}

func TestStateInitBaseAndInits(t *testing.T) {
	stb := &stateInitBase{
		ctx:     t.Context(),
		dataCon: harness.DataContext{},
	}

	sti := stateInit{
		keyPBFn: func(key, win []byte) *fnpb.StateKey {
			return &fnpb.StateKey{}
		},
		stateInitBase: stb,
	}
	_ = sti

	stmi := stateMapInit{
		valsPBFn: func(key, win, user []byte) *fnpb.StateKey {
			return &fnpb.StateKey{}
		},
		stateInitBase: stb,
	}
	_ = stmi
}

func TestStateOrderedList_Unit(t *testing.T) {
	sol := &StateOrderedList[int]{
		cache: newStateCache[[]orderedEntry[int]](),
		coder: coders.MakeCoder[int](),
	}
	ec := ElmC{keyBytes: "key", winBytes: "win", eventTime: time.UnixMilli(500)}

	// Append without timestamp uses ec.eventTime
	sol.Append(ec, 100)
	// AppendWithTimestamp
	sol.AppendWithTimestamp(ec, time.UnixMilli(100), 10)
	sol.AppendWithTimestamp(ec, time.UnixMilli(300), 30)

	t.Run("Read_All_Sorted", func(t *testing.T) {
		var readVals []int
		for v := range sol.Read(ec) {
			readVals = append(readVals, v)
		}
		if len(readVals) != 3 || readVals[0] != 10 || readVals[1] != 30 || readVals[2] != 100 {
			t.Errorf("Read() = %v, want [10, 30, 100]", readVals)
		}
	})

	t.Run("ReadRange_Zero_Times", func(t *testing.T) {
		var rangeVals []int
		for v := range sol.ReadRange(ec, time.Time{}, time.Time{}) {
			rangeVals = append(rangeVals, v)
		}
		if len(rangeVals) != 3 {
			t.Errorf("ReadRange count = %d, want 3", len(rangeVals))
		}
	})

	t.Run("ReadRangeEntries_Bounded", func(t *testing.T) {
		var rangeEntries []int
		for _, v := range sol.ReadRangeEntries(ec, time.UnixMilli(200), time.UnixMilli(600)) {
			rangeEntries = append(rangeEntries, v)
		}
		if len(rangeEntries) != 2 || rangeEntries[0] != 30 || rangeEntries[1] != 100 {
			t.Errorf("ReadRangeEntries = %v, want [30, 100]", rangeEntries)
		}
	})

	t.Run("All", func(t *testing.T) {
		var allVals []int
		for v := range sol.All(ec) {
			allVals = append(allVals, v)
		}
		if len(allVals) != 3 {
			t.Errorf("All() count = %d, want 3", len(allVals))
		}
	})

	t.Run("Clear", func(t *testing.T) {
		sol.Clear(ec)
		var postClear []int
		for v := range sol.Read(ec) {
			postClear = append(postClear, v)
		}
		if len(postClear) != 0 {
			t.Errorf("Read after Clear = %v, want []", postClear)
		}
	})
}

type dummyMerge struct{}

func (dummyMerge) MergeAccumulators(a, b int) int {
	return a + b
}

func TestStateCombining_Proto(t *testing.T) {
	comb := SimpleMerge(dummyMerge{})
	sc := AsStateCombining[int, int, int](comb)
	if sc.accessPatternUrn() != urnBagUserState {
		t.Errorf("accessPatternUrn() = %v, want %v", sc.accessPatternUrn(), urnBagUserState)
	}

	comps := &pipepb.Components{
		Coders: map[string]*pipepb.Coder{},
	}
	params := translateParams{
		InternedCoders: map[string]string{},
		Comps:          comps,
		TypeReg:        map[string]reflect.Type{},
	}

	spec := sc.toProtoParts(params)
	if spec.GetCombiningSpec() == nil {
		t.Fatalf("expected CombiningSpec in StateSpec")
	}
	if spec.GetCombiningSpec().GetAccumulatorCoderId() == "" {
		t.Errorf("expected non-empty AccumulatorCoderId")
	}
}

func TestStatePersist_Empty(t *testing.T) {
	tests := []struct {
		name    string
		persist func() error
	}{
		{
			name: "StateBag",
			persist: func() error {
				sb := &StateBag[int]{cache: newStateCache[[]int](), coder: coders.MakeCoder[int]()}
				return sb.persist()
			},
		},
		{
			name: "StateValue",
			persist: func() error {
				sv := &StateValue[int]{cache: newStateCache[int](), coder: coders.MakeCoder[int]()}
				return sv.persist()
			},
		},
		{
			name: "StateMap",
			persist: func() error {
				sm := &StateMap[string, int]{
					cache:    newStateCache[map[string]stateCacheEntry[int]](),
					valCoder: coders.MakeCoder[int](),
					keyCoder: coders.MakeCoder[string](),
				}
				return sm.persist()
			},
		},
		{
			name: "StateSet",
			persist: func() error {
				ss := &StateSet[int]{
					cache: newStateCache[map[int]stateCacheEntry[struct{}]](),
					coder: coders.MakeCoder[int](),
				}
				return ss.persist()
			},
		},
		{
			name: "StateMultiMap",
			persist: func() error {
				smm := &StateMultiMap[string, int]{
					cache:    newStateCache[map[string]stateCacheEntry[[]int]](),
					valCoder: coders.MakeCoder[int](),
					keyCoder: coders.MakeCoder[string](),
				}
				return smm.persist()
			},
		},
		{
			name: "StateOrderedList",
			persist: func() error {
				sol := &StateOrderedList[int]{
					cache: newStateCache[[]orderedEntry[int]](),
					coder: coders.MakeCoder[int](),
				}
				return sol.persist()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.persist(); err != nil {
				t.Errorf("persist on empty %s failed: %v", tc.name, err)
			}
		})
	}
}
