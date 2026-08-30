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
	"testing"
	"time"

	"lostluck.dev/beam-go/window"
)

type TimestampedIntSourceFn struct {
	Count  int
	Step   time.Duration
	Output PCol[int]
}

func (fn *TimestampedIntSourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		t0 := time.Unix(0, 0).UTC()
		for i := 0; i < fn.Count; i++ {
			subEC := ElmC{
				elmContext: elmContext{
					eventTime: t0.Add(time.Duration(i) * fn.Step),
					windows:   ec.windows,
					window:    ec.window,
					pane:      ec.pane,
				},
				pcollections: ec.pcollections,
			}
			fn.Output.Emit(subEC, i)
		}
		return nil
	})
}

type IntervalWindowInspectorFn struct {
	Win ObserveWindow[window.IntervalWindow]

	Win1Count CounterInt64
	Win2Count CounterInt64

	Out PCol[int]
}

func (fn *IntervalWindowInspectorFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, elm int) error {
		w := fn.Win.Of(ec)
		if w.Start.Equal(time.Unix(0, 0).UTC()) {
			fn.Win1Count.Inc(dfc, 1)
		} else if w.Start.Equal(time.Unix(10, 0).UTC()) {
			fn.Win2Count.Inc(dfc, 1)
		}
		fn.Out.Emit(ec, elm)
		return nil
	})
}

func TestWindow_FixedWindows_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		// Emit 4 elements: 0s, 5s (in [0s, 10s)) and 10s, 15s (in [10s, 20s))
		src := s.ParDo(imp, &TimestampedIntSourceFn{Count: 4, Step: 5 * time.Second})
		win := s.WindowInto(src.Output, window.FixedWindows(10*time.Second))
		ins := s.ParDo(win, &IntervalWindowInspectorFn{}, Name("inspector"))
		namedDiscard(s, ins.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got, want := int(pr.Counters["inspector.Win1Count"]), 2; got != want {
		t.Errorf("inspector.Win1Count = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["inspector.Win2Count"]), 2; got != want {
		t.Errorf("inspector.Win2Count = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 4; got != want {
		t.Errorf("sink.Processed = %v, want %v", got, want)
	}
}

type TimestampedKVSourceFn struct {
	Key    string
	Count  int
	Step   time.Duration
	Output PCol[KV[string, int]]
}

func (fn *TimestampedKVSourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		t0 := time.Unix(0, 0).UTC()
		for i := 0; i < fn.Count; i++ {
			subEC := ElmC{
				elmContext: elmContext{
					eventTime: t0.Add(time.Duration(i) * fn.Step),
					windows:   ec.windows,
					window:    ec.window,
					pane:      ec.pane,
				},
				pcollections: ec.pcollections,
			}
			fn.Output.Emit(subEC, KV[string, int]{Key: fn.Key, Value: 1})
		}
		return nil
	})
}

func TestWindow_CombinePerKey_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		// 4 elements for key "k": 0s, 5s (window 1) and 10s, 15s (window 2)
		src := s.ParDo(imp, &TimestampedKVSourceFn{Key: "k", Count: 4, Step: 5 * time.Second})
		win := s.WindowInto(src.Output, window.FixedWindows(10*time.Second))
		sums := s.CombinePerKey(win, SimpleMerge(SumFn[int]{}))
		namedDiscard(s, sums, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// Should have 2 output elements for key "k" (one per 10s window)
	if got, want := int(pr.Counters["sink.Processed"]), 2; got != want {
		t.Errorf("sink.Processed = %v, want %v (1 per window)", got, want)
	}
}

type WindowExplosionCounterFn struct {
	Win       ObserveWindow[window.IntervalWindow]
	ExecCount CounterInt64
	Out       PCol[int]
}

func (fn *WindowExplosionCounterFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, elm int) error {
		_ = fn.Win.Of(ec)
		fn.ExecCount.Inc(dfc, 1)
		fn.Out.Emit(ec, elm)
		return nil
	})
}

type NonObservingCounterFn struct {
	ExecCount CounterInt64
	Out       PCol[int]
}

func (fn *NonObservingCounterFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, elm int) error {
		fn.ExecCount.Inc(dfc, 1)
		fn.Out.Emit(ec, elm)
		return nil
	})
}

func TestWindow_SlidingWindows_Explosion_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		// 1 element at 5s in sliding window with period 20s and period step 10s
		// Belongs to [-10s, 10s) and [0s, 20s) -> 4 windows
		src := s.ParDo(imp, &TimestampedIntSourceFn{Count: 1, Step: 5 * time.Second})
		win := s.WindowInto(src.Output, window.SlidingWindows(20*time.Second, 5*time.Second))
		nonObs := s.ParDo(win, &NonObservingCounterFn{}, Name("nonObs"))
		obs := s.ParDo(nonObs.Out, &WindowExplosionCounterFn{}, Name("obs"))
		namedDiscard(s, obs.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// Non-observing DoFn executes 1 time for the batch multi-window element
	if got, want := int(pr.Counters["nonObs.ExecCount"]), 1; got != want {
		t.Errorf("nonObs.ExecCount = %v, want %v", got, want)
	}
	// Observing DoFn explodes the element and executes 4 times (once per window)
	if got, want := int(pr.Counters["obs.ExecCount"]), 4; got != want {
		t.Errorf("obs.ExecCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 4; got != want {
		t.Errorf("sink.Processed = %v, want %v", got, want)
	}
}

type SpecificTimestampKVSourceFn struct {
	Key        string
	Timestamps []time.Duration
	Output     PCol[KV[string, int]]
}

func (fn *SpecificTimestampKVSourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		t0 := time.Unix(0, 0).UTC()
		for _, offset := range fn.Timestamps {
			subEC := ElmC{
				elmContext: elmContext{
					eventTime: t0.Add(offset),
					windows:   ec.windows,
					window:    ec.window,
					pane:      ec.pane,
				},
				pcollections: ec.pcollections,
			}
			fn.Output.Emit(subEC, KV[string, int]{Key: fn.Key, Value: 1})
		}
		return nil
	})
}

func TestWindow_Sessions_CombinePerKey_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		// 3 elements: 0s, 2s (within 5s gap -> merge into 1 session) and 20s (separate session)
		src := s.ParDo(imp, &SpecificTimestampKVSourceFn{
			Key:        "sessionKey",
			Timestamps: []time.Duration{0, 2 * time.Second, 20 * time.Second},
		})
		win := s.WindowInto(src.Output, window.Sessions(5*time.Second))
		sums := s.CombinePerKey(win, SimpleMerge(SumFn[int]{}))
		namedDiscard(s, sums, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// Should have 2 output elements for key "sessionKey" corresponding to the 2 merged sessions
	if got, want := int(pr.Counters["sink.Processed"]), 2; got != want {
		t.Errorf("sink.Processed = %v, want %v (1 per merged session)", got, want)
	}
}

type PaneInspectorFn struct {
	Pane ObservePane

	PaneCount CounterInt64
	Out       PCol[int]
}

func (fn *PaneInspectorFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, elm int) error {
		p := fn.Pane.Of(ec)
		if p.IsFirst {
			fn.PaneCount.Inc(dfc, 1)
		}
		fn.Out.Emit(ec, elm)
		return nil
	})
}

func TestWindow_ObservePane_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &TimestampedIntSourceFn{Count: 3, Step: 1 * time.Second})
		win := s.WindowInto(src.Output, window.FixedWindows(10*time.Second))
		ins := s.ParDo(win, &PaneInspectorFn{}, Name("paneInspector"))
		namedDiscard(s, ins.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got, want := int(pr.Counters["paneInspector.PaneCount"]), 3; got != want {
		t.Errorf("paneInspector.PaneCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 3; got != want {
		t.Errorf("sink.Processed = %v, want %v", got, want)
	}
}
