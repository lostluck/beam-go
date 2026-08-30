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

	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
	"lostluck.dev/beam-go/window/trigger"
)

func TestWindowInto_TranslationTable(t *testing.T) {
	tests := []struct {
		name                 string
		buildPipeline        func(s *Scope)
		wantTransformUrn     string
		wantWindowFnUrn      string
		wantMergeStatus      pipepb.MergeStatus_Enum
		wantAccumulationMode pipepb.AccumulationMode_Enum
		wantWindowCoderUrn   string
		checkTrigger         func(t *testing.T, trig *pipepb.Trigger)
	}{
		{
			name: "FixedWindows_DefaultOptions",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				_ = s.WindowInto(imp, window.FixedWindows(10*time.Second))
			},
			wantTransformUrn:     "beam:transform:window_into:v1",
			wantWindowFnUrn:      "beam:window_fn:fixed_windows:v1",
			wantMergeStatus:      pipepb.MergeStatus_NON_MERGING,
			wantAccumulationMode: pipepb.AccumulationMode_DISCARDING,
			wantWindowCoderUrn:   "beam:coder:interval_window:v1",
			checkTrigger: func(t *testing.T, trig *pipepb.Trigger) {
				if trig.GetDefault() == nil {
					t.Errorf("expected Default trigger, got %v", trig)
				}
			},
		},
		{
			name: "SlidingWindows_WithAccumulatingAndCountTrigger",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				_ = s.WindowInto(
					imp,
					window.SlidingWindows(30*time.Second, 10*time.Second),
					window.Accumulating(),
					window.Trigger(trigger.AfterCount(5)),
					window.AllowedLateness(1*time.Minute),
				)
			},
			wantTransformUrn:     "beam:transform:window_into:v1",
			wantWindowFnUrn:      "beam:window_fn:sliding_windows:v1",
			wantMergeStatus:      pipepb.MergeStatus_NON_MERGING,
			wantAccumulationMode: pipepb.AccumulationMode_ACCUMULATING,
			wantWindowCoderUrn:   "beam:coder:interval_window:v1",
			checkTrigger: func(t *testing.T, trig *pipepb.Trigger) {
				ec := trig.GetElementCount()
				if ec == nil || ec.GetElementCount() != 5 {
					t.Errorf("expected ElementCount=5, got %v", ec)
				}
			},
		},
		{
			name: "Sessions_Merging",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				_ = s.WindowInto(imp, window.Sessions(5*time.Minute))
			},
			wantTransformUrn:     "beam:transform:window_into:v1",
			wantWindowFnUrn:      "beam:window_fn:session_windows:v1",
			wantMergeStatus:      pipepb.MergeStatus_NEEDS_MERGE,
			wantAccumulationMode: pipepb.AccumulationMode_DISCARDING,
			wantWindowCoderUrn:   "beam:coder:interval_window:v1",
			checkTrigger: func(t *testing.T, trig *pipepb.Trigger) {
				if trig.GetDefault() == nil {
					t.Errorf("expected Default trigger, got %v", trig)
				}
			},
		},
		{
			name: "GlobalWindows_WithWatermarkTrigger",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				_ = s.WindowInto(
					imp,
					window.GlobalWindows(),
					window.Trigger(
						trigger.AfterWatermark().
							WithEarlyFirings(trigger.AfterCount(10)).
							WithLateFirings(trigger.AfterCount(1)),
					),
				)
			},
			wantTransformUrn:     "beam:transform:window_into:v1",
			wantWindowFnUrn:      "beam:window_fn:global_windows:v1",
			wantMergeStatus:      pipepb.MergeStatus_NON_MERGING,
			wantAccumulationMode: pipepb.AccumulationMode_DISCARDING,
			wantWindowCoderUrn:   "beam:coder:global_window:v1",
			checkTrigger: func(t *testing.T, trig *pipepb.Trigger) {
				eow := trig.GetAfterEndOfWindow()
				if eow == nil {
					t.Fatalf("expected AfterEndOfWindow trigger, got %v", trig)
				}
				if eow.GetEarlyFirings().GetElementCount().GetElementCount() != 10 {
					t.Errorf("expected early firing count 10, got %v", eow.GetEarlyFirings())
				}
				if eow.GetLateFirings().GetElementCount().GetElementCount() != 1 {
					t.Errorf("expected late firing count 1, got %v", eow.GetLateFirings())
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scope{g: &graph{}}
			tc.buildPipeline(s)

			pipe := s.g.marshal(map[string]reflect.Type{})
			comps := pipe.GetComponents()

			// 1. Verify WindowInto PTransform exists
			var foundTransform bool
			for _, pt := range comps.GetTransforms() {
				if pt.GetSpec().GetUrn() == tc.wantTransformUrn {
					foundTransform = true
					break
				}
			}
			if !foundTransform {
				t.Fatalf("transform with URN %q not found in pipeline components", tc.wantTransformUrn)
			}

			// 2. Verify WindowingStrategy on the output PCollection
			pcol := comps.GetPcollections()["n1"]
			if pcol == nil {
				t.Fatalf("expected PCollection n1 in components, got %v", comps.GetPcollections())
			}
			matchingStrategy := comps.GetWindowingStrategies()[pcol.GetWindowingStrategyId()]
			if matchingStrategy == nil {
				t.Fatalf("strategy ID %q not found in components", pcol.GetWindowingStrategyId())
			}

			if matchingStrategy.GetWindowFn().GetUrn() != tc.wantWindowFnUrn {
				t.Errorf("WindowFn URN = %v, want %v", matchingStrategy.GetWindowFn().GetUrn(), tc.wantWindowFnUrn)
			}
			if matchingStrategy.GetMergeStatus() != tc.wantMergeStatus {
				t.Errorf("MergeStatus = %v, want %v", matchingStrategy.GetMergeStatus(), tc.wantMergeStatus)
			}
			if matchingStrategy.GetAccumulationMode() != tc.wantAccumulationMode {
				t.Errorf("AccumulationMode = %v, want %v", matchingStrategy.GetAccumulationMode(), tc.wantAccumulationMode)
			}

			// 3. Verify Window Coder
			coderID := matchingStrategy.GetWindowCoderId()
			windowCoder, ok := comps.GetCoders()[coderID]
			if !ok {
				t.Fatalf("window coder ID %q not found in components", coderID)
			}
			if windowCoder.GetSpec().GetUrn() != tc.wantWindowCoderUrn {
				t.Errorf("Window coder URN = %q, want %q", windowCoder.GetSpec().GetUrn(), tc.wantWindowCoderUrn)
			}

			// 4. Verify Trigger
			if tc.checkTrigger != nil {
				tc.checkTrigger(t, matchingStrategy.GetTrigger())
			}
		})
	}
}

func TestWindowInto_StrategyDeduplication(t *testing.T) {
	s := &Scope{g: &graph{}}
	imp := s.Impulse()
	w1 := s.WindowInto(imp, window.FixedWindows(10*time.Second))
	w2 := s.WindowInto(imp, window.FixedWindows(10*time.Second))
	_ = w1
	_ = w2

	pipe := s.g.marshal(map[string]reflect.Type{})
	comps := pipe.GetComponents()

	// Both w1 and w2 should share the same windowing strategy ID
	pcol1 := comps.GetPcollections()["n1"]
	pcol2 := comps.GetPcollections()["n2"]

	if pcol1 == nil || pcol2 == nil {
		t.Fatalf("expected PCollections n1 and n2, got %v", comps.GetPcollections())
	}
	if pcol1.GetWindowingStrategyId() != pcol2.GetWindowingStrategyId() {
		t.Errorf("expected identical WindowingStrategyId, got %q vs %q", pcol1.GetWindowingStrategyId(), pcol2.GetWindowingStrategyId())
	}
}
