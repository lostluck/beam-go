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
	"lostluck.dev/beam-go/window"
)

type observingTestDoFn struct {
	Win  ObserveWindow[window.IntervalWindow]
	Pane ObservePane

	observedWindows []window.IntervalWindow
	observedPanes   []coders.PaneInfo
	observedElms    []string
}

func (fn *observingTestDoFn) ProcessBundle(dfc *DFC[string]) error {
	return dfc.Process(func(ec ElmC, elm string) error {
		fn.observedWindows = append(fn.observedWindows, fn.Win.Of(ec))
		fn.observedPanes = append(fn.observedPanes, fn.Pane.Of(ec))
		fn.observedElms = append(fn.observedElms, elm)
		return nil
	})
}

type nonObservingTestDoFn struct {
	processedCount int
	receivedElms   []string
}

func (fn *nonObservingTestDoFn) ProcessBundle(dfc *DFC[string]) error {
	return dfc.Process(func(ec ElmC, elm string) error {
		fn.processedCount++
		fn.receivedElms = append(fn.receivedElms, elm)
		return nil
	})
}

func TestDFC_WindowExplosion_Table(t *testing.T) {
	tests := []struct {
		name        string
		inWindows   []window.BoundedWindow
		inPane      coders.PaneInfo
		wantWindows []window.IntervalWindow
		wantPanes   []coders.PaneInfo
	}{
		{
			name: "SingleWindow_IntervalWindow",
			inWindows: []window.BoundedWindow{
				window.IntervalWindow{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			inPane: coders.PaneInfo{IsFirst: true, IsLast: true, Timing: coders.TimingOnTime, Index: 0, NonSpeculativeIndex: 0},
			wantWindows: []window.IntervalWindow{
				{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			wantPanes: []coders.PaneInfo{{IsFirst: true, IsLast: true, Timing: coders.TimingOnTime, Index: 0, NonSpeculativeIndex: 0}},
		},
		{
			name: "MultiWindow_ExplodesEachWindow_WhenObserving",
			inWindows: []window.BoundedWindow{
				window.IntervalWindow{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
				window.IntervalWindow{
					Start: time.Unix(5, 0).UTC(),
					End:   time.Unix(15, 0).UTC(),
				},
				window.IntervalWindow{
					Start: time.Unix(10, 0).UTC(),
					End:   time.Unix(20, 0).UTC(),
				},
			},
			inPane: coders.PaneInfo{IsFirst: true, IsLast: false, Timing: coders.TimingEarly, Index: 0, NonSpeculativeIndex: -1},
			wantWindows: []window.IntervalWindow{
				{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
				{
					Start: time.Unix(5, 0).UTC(),
					End:   time.Unix(15, 0).UTC(),
				},
				{
					Start: time.Unix(10, 0).UTC(),
					End:   time.Unix(20, 0).UTC(),
				},
			},
			wantPanes: []coders.PaneInfo{
				{IsFirst: true, IsLast: false, Timing: coders.TimingEarly, Index: 0, NonSpeculativeIndex: -1},
				{IsFirst: true, IsLast: false, Timing: coders.TimingEarly, Index: 0, NonSpeculativeIndex: -1},
				{IsFirst: true, IsLast: false, Timing: coders.TimingEarly, Index: 0, NonSpeculativeIndex: -1},
			},
		},
		{
			name: "EarlyFiring_WithSpeculativeNegativeIndex",
			inWindows: []window.BoundedWindow{
				window.IntervalWindow{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			inPane: coders.PaneInfo{
				IsFirst:             true,
				IsLast:              false,
				Timing:              coders.TimingEarly,
				Index:               0,
				NonSpeculativeIndex: -1,
			},
			wantWindows: []window.IntervalWindow{
				{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			wantPanes: []coders.PaneInfo{
				{
					IsFirst:             true,
					IsLast:              false,
					Timing:              coders.TimingEarly,
					Index:               0,
					NonSpeculativeIndex: -1,
				},
			},
		},
		{
			name: "OnTimeFiring_FinalPane",
			inWindows: []window.BoundedWindow{
				window.IntervalWindow{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			inPane: coders.PaneInfo{
				IsFirst:             false,
				IsLast:              true,
				Timing:              coders.TimingOnTime,
				Index:               1,
				NonSpeculativeIndex: 0,
			},
			wantWindows: []window.IntervalWindow{
				{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			wantPanes: []coders.PaneInfo{
				{
					IsFirst:             false,
					IsLast:              true,
					Timing:              coders.TimingOnTime,
					Index:               1,
					NonSpeculativeIndex: 0,
				},
			},
		},
		{
			name: "LateFiring_AccumulatedIndex",
			inWindows: []window.BoundedWindow{
				window.IntervalWindow{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			inPane: coders.PaneInfo{
				IsFirst:             false,
				IsLast:              false,
				Timing:              coders.TimingLate,
				Index:               2,
				NonSpeculativeIndex: 1,
			},
			wantWindows: []window.IntervalWindow{
				{
					Start: time.Unix(0, 0).UTC(),
					End:   time.Unix(10, 0).UTC(),
				},
			},
			wantPanes: []coders.PaneInfo{
				{
					IsFirst:             false,
					IsLast:              false,
					Timing:              coders.TimingLate,
					Index:               2,
					NonSpeculativeIndex: 1,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dofn := &observingTestDoFn{}
			dfc := &DFC[string]{}
			dfc.update(0, "test", dofn, nil, nil, nil)
			if err := dofn.ProcessBundle(dfc); err != nil {
				t.Fatalf("ProcessBundle failed: %v", err)
			}

			if !dfc.observesWindows {
				t.Fatalf("expected dfc.observesWindows = true")
			}

			inputEC := ElmC{
				eventTime: time.Unix(7, 0).UTC(),
				windows:   tc.inWindows,
				pane:      tc.inPane,
			}

			if err := dfc.processElement(inputEC, "test-element"); err != nil {
				t.Fatalf("processElement failed: %v", err)
			}

			if !reflect.DeepEqual(dofn.observedWindows, tc.wantWindows) {
				t.Errorf("observedWindows = %#v, want %#v", dofn.observedWindows, tc.wantWindows)
			}
			if !reflect.DeepEqual(dofn.observedPanes, tc.wantPanes) {
				t.Errorf("observedPanes = %#v, want %#v", dofn.observedPanes, tc.wantPanes)
			}
			if len(dofn.observedElms) != len(tc.wantWindows) {
				t.Errorf("got %d element calls, want %d", len(dofn.observedElms), len(tc.wantWindows))
			}
		})
	}
}

func TestDFC_NoWindowExplosion_WhenNotObserving(t *testing.T) {
	dofn := &nonObservingTestDoFn{}
	dfc := &DFC[string]{}
	dfc.update(0, "test", dofn, nil, nil, nil)
	if err := dofn.ProcessBundle(dfc); err != nil {
		t.Fatalf("ProcessBundle failed: %v", err)
	}

	if dfc.observesWindows {
		t.Fatalf("expected dfc.observesWindows = false")
	}

	multiWindows := []window.BoundedWindow{
		window.IntervalWindow{Start: time.Unix(0, 0).UTC(), End: time.Unix(10, 0).UTC()},
		window.IntervalWindow{Start: time.Unix(5, 0).UTC(), End: time.Unix(15, 0).UTC()},
		window.IntervalWindow{Start: time.Unix(10, 0).UTC(), End: time.Unix(20, 0).UTC()},
	}

	inputEC := ElmC{
		eventTime: time.Unix(7, 0).UTC(),
		windows:   multiWindows,
		pane:      coders.NoFiringPane,
	}

	if err := dfc.processElement(inputEC, "batch-element"); err != nil {
		t.Fatalf("processElement failed: %v", err)
	}

	// Should NOT explode into 3 calls, but execute once with batch windows preserved!
	if dofn.processedCount != 1 {
		t.Errorf("processedCount = %d, want 1 (no explosion)", dofn.processedCount)
	}
	if len(dofn.receivedElms) != 1 || dofn.receivedElms[0] != "batch-element" {
		t.Errorf("receivedElms = %v, want ['batch-element']", dofn.receivedElms)
	}
}
