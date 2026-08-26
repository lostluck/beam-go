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

package synthetic

import (
	"iter"
	"testing"
	"time"

	"lostluck.dev/beam-go"
)

type countFn[E beam.Element] struct {
	Count beam.CounterInt64
}

func (fn *countFn[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	return dfc.Process(func(ec beam.ElmC, e E) error {
		fn.Count.Inc(dfc, 1)
		return nil
	})
}

func TestSyntheticStep(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name          string
		recordsPerIn  uint
		filterRatio   float64
		inputElements []string
		wantOutputs   int64
	}{
		{
			name:          "multiply_3x_no_filter",
			recordsPerIn:  3,
			filterRatio:   0.0,
			inputElements: []string{"a", "b", "c"},
			wantOutputs:   9,
		},
		{
			name:          "filtered_100_percent",
			recordsPerIn:  1,
			filterRatio:   1.0,
			inputElements: []string{"a", "b", "c"},
			wantOutputs:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := beam.LaunchAndWait(ctx, func(s *beam.Scope) error {
				in := beam.Create(s, tc.inputElements...)
				step := &syntheticStep[string]{
					PerElementDelay:             1 * time.Millisecond,
					PerBundleDelay:              5 * time.Millisecond,
					OutputRecordsPerInputRecord: tc.recordsPerIn,
					OutputFilterRatio:           tc.filterRatio,
				}
				out := beam.ParDo(s, in, step, beam.Name("synth"))
				beam.ParDo(s, out.Output, &countFn[string]{}, beam.Name("counter"))
				return nil
			})
			if err != nil {
				t.Fatalf("pipeline failed: %v", err)
			}
			if got := p.Counters["counter.Count"]; got != tc.wantOutputs {
				t.Errorf("got %v outputs, want %v", got, tc.wantOutputs)
			}
		})
	}
}

func TestSyntheticSourceRestrictionFactory(t *testing.T) {
	rf := syntheticSourceRestrictionFactory{}
	if err := rf.Setup(); err != nil {
		t.Errorf("Setup() returned error: %v", err)
	}

	t.Run("Produce", func(t *testing.T) {
		cfg := SourceConfig{
			NumRecords: 100,
		}
		rest := rf.Produce(cfg)
		if rest.Min != 0 || rest.Max != 100 {
			t.Errorf("Produce() = %+v, want {0, 100}", rest)
		}
	})

	t.Run("InitialSplit_Table", func(t *testing.T) {
		tests := []struct {
			name        string
			cfg         SourceConfig
			rest        beam.OffsetRange
			wantSplits  int
			shouldPanic bool
		}{
			{
				name: "split_num_bundles",
				cfg: SourceConfig{
					NumRecords:             100,
					KeySize:                10,
					ValueSize:              10,
					InitialSplitNumBundles: 4,
				},
				rest:       beam.OffsetRange{Min: 0, Max: 100},
				wantSplits: 4,
			},
			{
				name: "desired_bundle_size",
				cfg: SourceConfig{
					NumRecords:                    50,
					KeySize:                       5,
					ValueSize:                     5,
					InitialSplitNumBundles:        0,
					InitialSplitDesiredBundleSize: 100,
				},
				rest:       beam.OffsetRange{Min: 0, Max: 50},
				wantSplits: 5,
			},
			{
				name: "unsupported_zipf_panic",
				cfg: SourceConfig{
					InitialSplit: "zipf",
				},
				rest:        beam.OffsetRange{Min: 0, Max: 100},
				shouldPanic: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.shouldPanic {
					defer func() {
						if r := recover(); r == nil {
							t.Errorf("expected panic for %s", tc.name)
						}
					}()
				}

				splits := rf.InitialSplit(tc.cfg, tc.rest)
				var count int
				for r, w := range splits {
					count++
					if w != float64(r.Max-r.Min) {
						t.Errorf("weight mismatch: got %v, want %v", w, float64(r.Max-r.Min))
					}
				}
				if !tc.shouldPanic && count != tc.wantSplits {
					t.Errorf("got %d splits, want %d", count, tc.wantSplits)
				}
			})
		}
	})
}

type synthSDFRestrictionFactory struct{}

func (synthSDFRestrictionFactory) Setup() error { return nil }
func (synthSDFRestrictionFactory) InitialSplit(e string, r beam.OffsetRange) iter.Seq2[beam.OffsetRange, float64] {
	return func(yield func(beam.OffsetRange, float64) bool) {
		yield(r, float64(r.Max-r.Min))
	}
}
func (synthSDFRestrictionFactory) Produce(e string) beam.OffsetRange {
	return beam.OffsetRange{Min: 0, Max: 5}
}

func TestSyntheticSDFStep(t *testing.T) {
	ctx := t.Context()
	p, err := beam.LaunchAndWait(ctx, func(s *beam.Scope) error {
		in := beam.Create(s, "item")
		step := &syntheticSDFStep[synthSDFRestrictionFactory, *beam.ORTracker, string]{
			PerElementDelay: 1 * time.Millisecond,
			PerBundleDelay:  5 * time.Millisecond,
			MakeTracker: func(r beam.OffsetRange) *beam.ORTracker {
				return &beam.ORTracker{Rest: r}
			},
		}
		out := beam.ParDo(s, in, step, beam.Name("synth_sdf"))
		beam.ParDo(s, out.Output, &countFn[string]{}, beam.Name("counter"))
		return nil
	})
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if p.Counters["counter.Count"] != 5 {
		t.Errorf("got %v outputs, want 5", p.Counters["counter.Count"])
	}
}
