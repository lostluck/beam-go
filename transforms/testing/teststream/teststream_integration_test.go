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

package teststream_test

import (
	"fmt"
	"testing"
	"time"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/transforms/testing/passert"
	"lostluck.dev/beam-go/transforms/testing/teststream"
	"lostluck.dev/beam-go/window"
	"lostluck.dev/beam-go/window/trigger"
)

type DetailedPaneInspectorFn struct {
	Pane beam.ObservePane
	Win  beam.ObserveWindow[window.IntervalWindow]

	EarlyCount   beam.CounterInt64
	OnTimeCount  beam.CounterInt64
	LateCount    beam.CounterInt64
	UnknownCount beam.CounterInt64
	FirstCount   beam.CounterInt64
	LastCount    beam.CounterInt64
}

func (fn *DetailedPaneInspectorFn) ProcessBundle(dfc *beam.DFC[int]) error {
	return dfc.Process(func(ec beam.ElmC, elm int) error {
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
		return nil
	})
}

func pipeName(t *testing.T) beam.Options {
	return beam.Name(fmt.Sprintf("%s", t.Name()))
}

func TestTestStream_FixedWindows_Integration(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	pr, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[int](s).
			AddElements(t0, 1, 2).
			AdvanceWatermark(t0.Add(5 * time.Second)).
			AddElements(t0.Add(10*time.Second), 3, 4).
			AdvanceWatermark(t0.Add(15 * time.Second)).
			AdvanceWatermarkToInfinity().
			Build()
		win := s.WindowInto(stream, window.FixedWindows(10*time.Second))
		s.ParDo(win, &DetailedPaneInspectorFn{}, beam.Name("fixedInspector"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got := int(pr.Counters["fixedInspector.UnknownCount"]); got < 4 {
		t.Errorf("fixedInspector.UnknownCount = %v, want >= 4", got)
	}
	if got := int(pr.Counters["fixedInspector.FirstCount"]); got < 3 {
		t.Errorf("fixedInspector.FirstCount = %v, want >= 3", got)
	}
	if got := int(pr.Counters["fixedInspector.LastCount"]); got < 3 {
		t.Errorf("fixedInspector.LastCount = %v, want >= 3", got)
	}
}

type GroupedPaneInspectorFn struct {
	Pane beam.ObservePane
	Win  beam.ObserveWindow[window.IntervalWindow]

	EarlyCount   beam.CounterInt64
	OnTimeCount  beam.CounterInt64
	LateCount    beam.CounterInt64
	UnknownCount beam.CounterInt64
	FirstCount   beam.CounterInt64
	LastCount    beam.CounterInt64

	SumValues beam.CounterInt64
}

func (fn *GroupedPaneInspectorFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.Iter[int]]]) error {
	return dfc.Process(func(ec beam.ElmC, kv beam.KV[string, beam.Iter[int]]) error {
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
		for v := range kv.Value.All() {
			fn.SumValues.Inc(dfc, int64(v))
		}
		return nil
	})
}

type passThroughFn[E beam.Element] struct {
	Out beam.PCol[E]
}

func (fn *passThroughFn[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	return dfc.Process(func(ec beam.ElmC, elm E) error {
		fn.Out.Emit(ec, elm)
		return nil
	})
}

func TestTestStream_TriggerPanes_EarlyOnTimeLate_GBK(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	pr, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, int]](s).
			// Window [0s, 10s):
			// 1. Emit 2 elements -> triggers Early firing (AfterCount(2))
			AddElements(t0.Add(1*time.Second), beam.Pair("k", 10), beam.Pair("k", 20)).
			// 2. Advance watermark past 10s -> triggers OnTime firing for [0s, 10s)
			AdvanceWatermark(t0.Add(10 * time.Second)).
			// 3. Emit late element in [0s, 10s) -> triggers Late firing (AfterCount(1))
			AddElements(t0.Add(2*time.Second), beam.Pair("k", 30)).
			// 4. Close window
			AdvanceWatermarkToInfinity().
			Build()

		win := s.WindowInto(stream,
			window.FixedWindows(10*time.Second),
			window.Trigger(
				trigger.AfterWatermark().
					WithEarlyFirings(trigger.AfterCount(2)).
					WithLateFirings(trigger.AfterCount(1)),
			),
			window.Accumulating(),
			window.AllowedLateness(1*time.Minute),
		)
		pt := s.ParDo(win, &passThroughFn[beam.KV[string, int]]{})
		gbk := s.GBK(pt.Out)
		s.ParDo(gbk, &GroupedPaneInspectorFn{}, beam.Name("earlyLateInspector"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got := int(pr.Counters["earlyLateInspector.EarlyCount"]); got < 1 {
		t.Errorf("earlyLateInspector.EarlyCount = %v, want >= 1", got)
	}
	if got := int(pr.Counters["earlyLateInspector.OnTimeCount"]); got < 1 {
		t.Errorf("earlyLateInspector.OnTimeCount = %v, want >= 1", got)
	}
	if got := int(pr.Counters["earlyLateInspector.LateCount"]); got < 1 {
		t.Errorf("earlyLateInspector.LateCount = %v, want >= 1", got)
	}
	if got := int(pr.Counters["earlyLateInspector.FirstCount"]); got < 1 {
		t.Errorf("earlyLateInspector.FirstCount = %v, want >= 1", got)
	}
	// In Accumulating mode, sum of values across early, on-time, and late panes is at least 90 (30 + 60)
	if got := int(pr.Counters["earlyLateInspector.SumValues"]); got < 90 {
		t.Errorf("earlyLateInspector.SumValues = %v, want >= 90", got)
	}
}

func TestTestStream_TriggerPanes_Discarding_GBK(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	pr, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, int]](s).
			// Window [0s, 10s):
			// 1. Emit 2 elements -> triggers Early firing (AfterCount(2))
			AddElements(t0.Add(1*time.Second), beam.Pair("k", 10), beam.Pair("k", 20)).
			// 2. Advance watermark past 10s -> triggers OnTime firing for [0s, 10s)
			AdvanceWatermark(t0.Add(10 * time.Second)).
			// 3. Emit late element in [0s, 10s) -> triggers Late firing (AfterCount(1))
			AddElements(t0.Add(2*time.Second), beam.Pair("k", 30)).
			// 4. Close window
			AdvanceWatermarkToInfinity().
			Build()

		win := s.WindowInto(stream,
			window.FixedWindows(10*time.Second),
			window.Trigger(
				trigger.AfterWatermark().
					WithEarlyFirings(trigger.AfterCount(2)).
					WithLateFirings(trigger.AfterCount(1)),
			),
			window.Discarding(),
			window.AllowedLateness(1*time.Minute),
		)
		pt := s.ParDo(win, &passThroughFn[beam.KV[string, int]]{})
		gbk := s.GBK(pt.Out)
		s.ParDo(gbk, &GroupedPaneInspectorFn{}, beam.Name("discardInspector"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got := int(pr.Counters["discardInspector.EarlyCount"]); got < 1 {
		t.Errorf("discardInspector.EarlyCount = %v, want >= 1", got)
	}
	if got := int(pr.Counters["discardInspector.LateCount"]); got < 1 {
		t.Errorf("discardInspector.LateCount = %v, want >= 1", got)
	}
	if got := int(pr.Counters["discardInspector.FirstCount"]); got < 1 {
		t.Errorf("discardInspector.FirstCount = %v, want >= 1", got)
	}
	// In Discarding mode: early pane (30) + late pane (30) = 60
	if got := int(pr.Counters["discardInspector.SumValues"]); got < 50 {
		t.Errorf("discardInspector.SumValues = %v, want >= 50", got)
	}
}

type MultiWindowPaneInspectorFn struct {
	Pane beam.ObservePane
	Win  beam.ObserveWindow[window.IntervalWindow]

	Win1OnTime beam.CounterInt64
	Win1Late   beam.CounterInt64
	Win2OnTime beam.CounterInt64
	Win2Late   beam.CounterInt64
}

func (fn *MultiWindowPaneInspectorFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.Iter[int]]]) error {
	return dfc.Process(func(ec beam.ElmC, kv beam.KV[string, beam.Iter[int]]) error {
		w := fn.Win.Of(ec)
		p := fn.Pane.Of(ec)
		for range kv.Value.All() {
		}
		if w.Start.Equal(time.Unix(0, 0).UTC()) {
			if p.Timing == coders.TimingOnTime {
				fn.Win1OnTime.Inc(dfc, 1)
			} else if p.Timing == coders.TimingLate {
				fn.Win1Late.Inc(dfc, 1)
			}
		} else if w.Start.Equal(time.Unix(10, 0).UTC()) {
			if p.Timing == coders.TimingOnTime {
				fn.Win2OnTime.Inc(dfc, 1)
			} else if p.Timing == coders.TimingLate {
				fn.Win2Late.Inc(dfc, 1)
			}
		}
		return nil
	})
}

func TestTestStream_MultipleWindows_WatermarkProgression(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	pr, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, int]](s).
			// Window 1 [0s, 10s)
			AddElements(t0.Add(1*time.Second), beam.Pair("k", 10)).
			// Window 2 [10s, 20s)
			AddElements(t0.Add(12*time.Second), beam.Pair("k", 20)).
			// Advance watermark to 10s: closes on-time for Window 1, Window 2 remains open
			AdvanceWatermark(t0.Add(10 * time.Second)).
			// Late element for Window 1, on-time element for Window 2
			AddElements(t0.Add(2*time.Second), beam.Pair("k", 15)).
			AddElements(t0.Add(15*time.Second), beam.Pair("k", 25)).
			// Advance watermark to 20s: closes Window 1 late and Window 2 on-time
			AdvanceWatermark(t0.Add(20 * time.Second)).
			AdvanceWatermarkToInfinity().
			Build()

		win := s.WindowInto(stream,
			window.FixedWindows(10*time.Second),
			window.Trigger(
				trigger.AfterWatermark().
					WithLateFirings(trigger.AfterCount(1)),
			),
			window.Accumulating(),
			window.AllowedLateness(1*time.Minute),
		)
		pt := s.ParDo(win, &passThroughFn[beam.KV[string, int]]{})
		gbk := s.GBK(pt.Out)
		s.ParDo(gbk, &MultiWindowPaneInspectorFn{}, beam.Name("multiPaneInspector"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if got, want := int(pr.Counters["multiPaneInspector.Win1OnTime"]), 1; got != want {
		t.Errorf("multiPaneInspector.Win1OnTime = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["multiPaneInspector.Win1Late"]), 1; got != want {
		t.Errorf("multiPaneInspector.Win1Late = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["multiPaneInspector.Win2OnTime"]), 1; got != want {
		t.Errorf("multiPaneInspector.Win2OnTime = %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["multiPaneInspector.Win2Late"]), 1; got != want {
		t.Errorf("multiPaneInspector.Win2Late = %v, want %v", got, want)
	}
}

func TestTestStream_Passert_Integration(t *testing.T) {
	t0 := time.Unix(100, 0).UTC()
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[string](s).
			AddElements(t0, "hello", "world").
			AdvanceWatermark(t0.Add(5 * time.Second)).
			AddElements(t0.Add(10*time.Second), "beam").
			AdvanceWatermarkToInfinity().
			Build()

		passert.Equals(s, stream, "hello", "world", "beam")
		passert.Count(s, stream, 3)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}
