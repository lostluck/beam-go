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

package window

import (
	"testing"
	"time"

	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window/trigger"
)

func TestWindowFn_AssignWindows(t *testing.T) {
	t.Run("GlobalWindows", func(t *testing.T) {
		fn := GlobalWindows()
		wins := fn.AssignWindows(time.UnixMilli(12345))
		if len(wins) != 1 {
			t.Fatalf("got %d windows, want 1", len(wins))
		}
		if _, ok := wins[0].(GlobalWindow); !ok {
			t.Errorf("got %v, want GlobalWindow", wins[0])
		}
		if fn.WindowCoderURN() != "beam:coder:global_window:v1" {
			t.Errorf("got coder %v, want global window coder", fn.WindowCoderURN())
		}
		if fn.MergeStatus() != pipepb.MergeStatus_NON_MERGING {
			t.Errorf("got merge status %v, want NON_MERGING", fn.MergeStatus())
		}
		if !fn.AssignsToOneWindow() {
			t.Errorf("expected AssignsToOneWindow=true")
		}
		if spec := fn.ToProto(); spec.GetUrn() != "beam:window_fn:global_windows:v1" {
			t.Errorf("got URN %v, want global windows URN", spec.GetUrn())
		}
	})

	t.Run("FixedWindows", func(t *testing.T) {
		tests := []struct {
			name        string
			size        time.Duration
			offset      time.Duration
			timestamp   time.Time
			wantStart   time.Time
			wantEnd     time.Time
		}{
			{
				name:      "aligned 10s window",
				size:      10 * time.Second,
				offset:    0,
				timestamp: time.UnixMilli(15000),
				wantStart: time.UnixMilli(10000),
				wantEnd:   time.UnixMilli(20000),
			},
			{
				name:      "offset 2s in 10s window",
				size:      10 * time.Second,
				offset:    2 * time.Second,
				timestamp: time.UnixMilli(15000),
				wantStart: time.UnixMilli(12000),
				wantEnd:   time.UnixMilli(22000),
			},
			{
				name:      "exact boundary timestamp",
				size:      5 * time.Second,
				offset:    0,
				timestamp: time.UnixMilli(10000),
				wantStart: time.UnixMilli(10000),
				wantEnd:   time.UnixMilli(15000),
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				fn := FixedWindows(tc.size, tc.offset)
				wins := fn.AssignWindows(tc.timestamp)
				if len(wins) != 1 {
					t.Fatalf("got %d windows, want 1", len(wins))
				}
				iw, ok := wins[0].(IntervalWindow)
				if !ok {
					t.Fatalf("got %T, want IntervalWindow", wins[0])
				}
				if !iw.Start.Equal(tc.wantStart) || !iw.End.Equal(tc.wantEnd) {
					t.Errorf("got [%v, %v), want [%v, %v)", iw.Start, iw.End, tc.wantStart, tc.wantEnd)
				}
			})
		}
	})

	t.Run("SlidingWindows", func(t *testing.T) {
		fn := SlidingWindows(10*time.Second, 5*time.Second)
		wins := fn.AssignWindows(time.UnixMilli(7000))
		if len(wins) != 2 {
			t.Fatalf("got %d windows, want 2", len(wins))
		}
		iw0 := wins[0].(IntervalWindow)
		iw1 := wins[1].(IntervalWindow)

		if !iw0.Start.Equal(time.UnixMilli(5000)) || !iw0.End.Equal(time.UnixMilli(15000)) {
			t.Errorf("window 0 = [%v, %v), want [5000, 15000)", iw0.Start, iw0.End)
		}
		if !iw1.Start.Equal(time.UnixMilli(0)) || !iw1.End.Equal(time.UnixMilli(10000)) {
			t.Errorf("window 1 = [%v, %v), want [0, 10000)", iw1.Start, iw1.End)
		}
	})

	t.Run("Sessions", func(t *testing.T) {
		fn := Sessions(10 * time.Minute)
		timestamp := time.UnixMilli(50000)
		wins := fn.AssignWindows(timestamp)
		if len(wins) != 1 {
			t.Fatalf("got %d windows, want 1", len(wins))
		}
		iw := wins[0].(IntervalWindow)
		if !iw.Start.Equal(timestamp) || !iw.End.Equal(timestamp.Add(10*time.Minute)) {
			t.Errorf("got [%v, %v), want [%v, %v)", iw.Start, iw.End, timestamp, timestamp.Add(10*time.Minute))
		}
		if fn.MergeStatus() != pipepb.MergeStatus_NEEDS_MERGE {
			t.Errorf("got merge status %v, want NEEDS_MERGE", fn.MergeStatus())
		}
	})
}

func TestStrategy_Options(t *testing.T) {
	customTrigger := trigger.AfterCount(5)
	strat := NewStrategy(
		FixedWindows(time.Minute),
		Trigger(customTrigger),
		Accumulating(),
		AllowedLateness(10*time.Minute),
		OutputTimeLatest(),
	)

	if strat.AccumulationMode != pipepb.AccumulationMode_ACCUMULATING {
		t.Errorf("got accumulation mode %v, want ACCUMULATING", strat.AccumulationMode)
	}
	if strat.AllowedLateness != 10*time.Minute {
		t.Errorf("got allowed lateness %v, want 10m", strat.AllowedLateness)
	}
	if strat.OutputTime != pipepb.OutputTime_LATEST_IN_PANE {
		t.Errorf("got output time %v, want LATEST_IN_PANE", strat.OutputTime)
	}
}
