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

type sumIntCombiner struct{}

func (sumIntCombiner) CreateAccumulator() int {
	return 0
}

func (sumIntCombiner) AddInput(accum, val int) int {
	return accum + val
}

func (sumIntCombiner) MergeAccumulators(a, b int) int {
	return a + b
}

func (sumIntCombiner) ExtractOutput(accum int) int {
	return accum
}

func TestLiftedAddingCombine_WindowPartitioning_Table(t *testing.T) {
	w1 := window.IntervalWindow{Start: time.Unix(0, 0).UTC(), End: time.Unix(10, 0).UTC()}
	w2 := window.IntervalWindow{Start: time.Unix(10, 0).UTC(), End: time.Unix(20, 0).UTC()}

	tests := []struct {
		name     string
		inputs   []struct {
			key       string
			val       int
			window    window.BoundedWindow
			eventTime time.Time
		}
		wantOutputs []struct {
			key       string
			accum     int
			window    window.BoundedWindow
			eventTime time.Time
		}
	}{
		{
			name: "SameKey_DifferentWindows_SeparatelyAggregated",
			inputs: []struct {
				key       string
				val       int
				window    window.BoundedWindow
				eventTime time.Time
			}{
				{key: "a", val: 10, window: w1, eventTime: time.Unix(2, 0).UTC()},
				{key: "a", val: 20, window: w1, eventTime: time.Unix(3, 0).UTC()},
				{key: "a", val: 5, window: w2, eventTime: time.Unix(12, 0).UTC()},
				{key: "a", val: 15, window: w2, eventTime: time.Unix(14, 0).UTC()},
				{key: "b", val: 7, window: w1, eventTime: time.Unix(4, 0).UTC()},
			},
			wantOutputs: []struct {
				key       string
				accum     int
				window    window.BoundedWindow
				eventTime time.Time
			}{
				{key: "a", accum: 30, window: w1, eventTime: time.Unix(2, 0).UTC()},
				{key: "a", accum: 20, window: w2, eventTime: time.Unix(12, 0).UTC()},
				{key: "b", accum: 7, window: w1, eventTime: time.Unix(4, 0).UTC()},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			combine := &liftedAddingCombine[string, int, int]{
				Merger: sumIntCombiner{},
			}

			dfc := &DFC[KV[string, int]]{}
			dfc.update(0, "precombine", combine, nil, nil, nil)

			var emitted []struct {
				key       string
				accum     int
				window    window.BoundedWindow
				eventTime time.Time
			}

			combine.Output = PCol[KV[string, int]]{
				valid:       true,
				globalIndex: 1,
			}

			// Capture downstream emissions through DFC downstream processor
			toConsumer := &DFC[KV[string, int]]{id: 1}
			_ = toConsumer.Process(func(ec ElmC, elm KV[string, int]) error {
				emitted = append(emitted, struct {
					key       string
					accum     int
					window    window.BoundedWindow
					eventTime time.Time
				}{
					key:       elm.Key,
					accum:     elm.Value,
					window:    ec.window,
					eventTime: ec.EventTime(),
				})
				return nil
			})
			dfc.downstream = []processor{toConsumer}

			if err := combine.ProcessBundle(dfc); err != nil {
				t.Fatalf("ProcessBundle failed: %v", err)
			}

			for _, in := range tc.inputs {
				ec := ElmC{
					elmContext: elmContext{
						eventTime: in.eventTime,
						windows:   []window.BoundedWindow{in.window},
						window:    in.window,
						pane:      coders.NoFiringPane,
					},
				}
				if err := dfc.processElement(ec, KV[string, int]{Key: in.key, Value: in.val}); err != nil {
					t.Fatalf("processElement failed: %v", err)
				}
			}

			// Finish bundle to flush combiner cache
			if err := dfc.finish(); err != nil {
				t.Fatalf("finish failed: %v", err)
			}

			// Compare maps/sets of outputs since map iteration order is non-deterministic
			if len(emitted) != len(tc.wantOutputs) {
				t.Fatalf("got %d emitted elements, want %d", len(emitted), len(tc.wantOutputs))
			}

			for _, want := range tc.wantOutputs {
				found := false
				for _, got := range emitted {
					if got.key == want.key && got.accum == want.accum && reflect.DeepEqual(got.window, want.window) && got.eventTime.Equal(want.eventTime) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("missing expected output %#v in emitted outputs: %#v", want, emitted)
				}
			}
		})
	}
}
