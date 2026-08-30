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

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/window"
	"lostluck.dev/beam-go/window/trigger"
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

type SpecificTimestampKVVal struct {
	Offset time.Duration
	Val    int
}

type SpecificTimestampKVValuesSourceFn struct {
	Key    string
	Values []SpecificTimestampKVVal
	Output PCol[KV[string, int]]
}

func (fn *SpecificTimestampKVValuesSourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		t0 := time.Unix(0, 0).UTC()
		for _, item := range fn.Values {
			subEC := ElmC{
				elmContext: elmContext{
					eventTime: t0.Add(item.Offset),
					windows:   ec.windows,
					window:    ec.window,
					pane:      ec.pane,
				},
				pcollections: ec.pcollections,
			}
			fn.Output.Emit(subEC, KV[string, int]{Key: fn.Key, Value: item.Val})
		}
		return nil
	})
}

type WindowedStateAccumDoFn struct {
	Sum StateValue[int]
	Out PCol[KV[string, int]]
}

func (fn *WindowedStateAccumDoFn) ProcessBundle(dfc *DFC[KV[string, int]]) error {
	return dfc.Process(func(ec ElmC, kv KV[string, int]) error {
		cur, _ := fn.Sum.Get(ec)
		next := cur + kv.Value
		fn.Sum.Set(ec, next)
		fn.Out.Emit(ec, KV[string, int]{Key: kv.Key, Value: next})
		return nil
	})
}

type StateOutputValidatorFn struct {
	Win1Sum10 CounterInt64
	Win1Sum15 CounterInt64
	Win2Sum20 CounterInt64
	Win2Sum23 CounterInt64
}

func (fn *StateOutputValidatorFn) ProcessBundle(dfc *DFC[KV[string, int]]) error {
	return dfc.Process(func(ec ElmC, kv KV[string, int]) error {
		switch kv.Value {
		case 10:
			fn.Win1Sum10.Inc(dfc, 1)
		case 15:
			fn.Win1Sum15.Inc(dfc, 1)
		case 20:
			fn.Win2Sum20.Inc(dfc, 1)
		case 23:
			fn.Win2Sum23.Inc(dfc, 1)
		}
		return nil
	})
}

func TestWindow_StatefulParDo_WindowPartitionedState_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		// Emit 4 elements for key "k":
		// Window 1 [0s, 10s): 0s val 10 (sum 10), 5s val 5 (sum 15)
		// Window 2 [10s, 20s): 10s val 20 (sum 20), 15s val 3 (sum 23)
		src := s.ParDo(imp, &SpecificTimestampKVValuesSourceFn{
			Key: "k",
			Values: []SpecificTimestampKVVal{
				{Offset: 0, Val: 10},
				{Offset: 5 * time.Second, Val: 5},
				{Offset: 10 * time.Second, Val: 20},
				{Offset: 15 * time.Second, Val: 3},
			},
		})
		win := s.WindowInto(src.Output, window.FixedWindows(10*time.Second))
		st := s.StatefulParDo(win, &WindowedStateAccumDoFn{})
		s.ParDo(st.Out, &StateOutputValidatorFn{}, Name("validator"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got, want := int(pr.Counters["validator.Win1Sum10"]), 1; got != want {
		t.Errorf("validator.Win1Sum10 = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["validator.Win1Sum15"]), 1; got != want {
		t.Errorf("validator.Win1Sum15 = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["validator.Win2Sum20"]), 1; got != want {
		t.Errorf("validator.Win2Sum20 = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["validator.Win2Sum23"]), 1; got != want {
		t.Errorf("validator.Win2Sum23 = %v, want %v", got, want)
	}
}

type DetailedPaneInspectorFn struct {
	Pane ObservePane

	EarlyCount   CounterInt64
	OnTimeCount  CounterInt64
	LateCount    CounterInt64
	UnknownCount CounterInt64
	FirstCount   CounterInt64
	LastCount    CounterInt64

	Out PCol[int]
}

func (fn *DetailedPaneInspectorFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, elm int) error {
		p := fn.Pane.Of(ec)
		switch p.Timing {
		case coders.TimingEarly:
			fn.EarlyCount.Inc(dfc, 1)
		case coders.TimingOnTime:
			fn.OnTimeCount.Inc(dfc, 1)
		case coders.TimingLate:
			fn.LateCount.Inc(dfc, 1)
		case coders.TimingUnknown:
			fn.UnknownCount.Inc(dfc, 1)
		}
		if p.IsFirst {
			fn.FirstCount.Inc(dfc, 1)
		}
		if p.IsLast {
			fn.LastCount.Inc(dfc, 1)
		}
		fn.Out.Emit(ec, elm)
		return nil
	})
}

func TestWindow_TriggerDSL_EarlyLateFirings_Pipeline(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &TimestampedIntSourceFn{Count: 6, Step: 2 * time.Second})
		win := s.WindowInto(src.Output,
			window.FixedWindows(10*time.Second),
			window.Trigger(
				trigger.AfterWatermark().
					WithEarlyFirings(trigger.AfterCount(2)).
					WithLateFirings(trigger.AfterCount(1)),
			),
			window.Accumulating(),
			window.AllowedLateness(1*time.Minute),
		)
		ins := s.ParDo(win, &DetailedPaneInspectorFn{}, Name("paneInspector"))
		namedDiscard(s, ins.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got, want := int(pr.Counters["paneInspector.UnknownCount"]), 6; got != want {
		t.Errorf("paneInspector.UnknownCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["paneInspector.OnTimeCount"]), 0; got != want {
		t.Errorf("paneInspector.OnTimeCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["paneInspector.EarlyCount"]), 0; got != want {
		t.Errorf("paneInspector.EarlyCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["paneInspector.LateCount"]), 0; got != want {
		t.Errorf("paneInspector.LateCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["paneInspector.FirstCount"]), 6; got != want {
		t.Errorf("paneInspector.FirstCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["paneInspector.LastCount"]), 6; got != want {
		t.Errorf("paneInspector.LastCount = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 6; got != want {
		t.Errorf("sink.Processed = %v, want %v", got, want)
	}
}

