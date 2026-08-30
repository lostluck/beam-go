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

	"lostluck.dev/beam-go/coders"
)

func TestGlobalWindow(t *testing.T) {
	tests := []struct {
		name        string
		gw          GlobalWindow
		other       BoundedWindow
		wantEquals  bool
		wantString  string
		wantMaxTime time.Time
	}{
		{
			name:        "equal global window",
			gw:          GlobalWindow{},
			other:       GlobalWindow{},
			wantEquals:  true,
			wantString:  "GlobalWindow",
			wantMaxTime: GlobalWindowMaxTimestamp,
		},
		{
			name:        "unequal interval window",
			gw:          GlobalWindow{},
			other:       IntervalWindow{Start: time.UnixMilli(0), End: time.UnixMilli(1000)},
			wantEquals:  false,
			wantString:  "GlobalWindow",
			wantMaxTime: GlobalWindowMaxTimestamp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.gw.Equals(tc.other); got != tc.wantEquals {
				t.Errorf("GlobalWindow.Equals(%v) = %v, want %v", tc.other, got, tc.wantEquals)
			}
			if got := tc.gw.String(); got != tc.wantString {
				t.Errorf("GlobalWindow.String() = %v, want %v", got, tc.wantString)
			}
			if got := tc.gw.MaxTimestamp(); !got.Equal(tc.wantMaxTime) {
				t.Errorf("GlobalWindow.MaxTimestamp() = %v, want %v", got, tc.wantMaxTime)
			}
		})
	}
}

func TestIntervalWindow_Properties(t *testing.T) {
	tests := []struct {
		name         string
		w            IntervalWindow
		wantDuration time.Duration
		wantMaxTime  time.Time
		wantString   string
	}{
		{
			name:         "1 second interval",
			w:            IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(2000)},
			wantDuration: 1 * time.Second,
			wantMaxTime:  time.UnixMilli(1999),
			wantString:   "[" + time.UnixMilli(1000).UTC().Format(time.RFC3339Nano) + ", " + time.UnixMilli(2000).UTC().Format(time.RFC3339Nano) + ")",
		},
		{
			name:         "1 hour interval",
			w:            IntervalWindow{Start: time.UnixMilli(0), End: time.UnixMilli(3600000)},
			wantDuration: 1 * time.Hour,
			wantMaxTime:  time.UnixMilli(3599999),
			wantString:   "[" + time.UnixMilli(0).UTC().Format(time.RFC3339Nano) + ", " + time.UnixMilli(3600000).UTC().Format(time.RFC3339Nano) + ")",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.w.Duration(); got != tc.wantDuration {
				t.Errorf("IntervalWindow.Duration() = %v, want %v", got, tc.wantDuration)
			}
			if got := tc.w.MaxTimestamp(); !got.Equal(tc.wantMaxTime) {
				t.Errorf("IntervalWindow.MaxTimestamp() = %v, want %v", got, tc.wantMaxTime)
			}
			if got := tc.w.String(); got != tc.wantString {
				t.Errorf("IntervalWindow.String() = %v, want %v", got, tc.wantString)
			}
		})
	}
}

func TestIntervalWindow_Contains(t *testing.T) {
	w := IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)}

	tests := []struct {
		name      string
		timestamp time.Time
		want      bool
	}{
		{"before start", time.UnixMilli(999), false},
		{"at start", time.UnixMilli(1000), true},
		{"inside interval", time.UnixMilli(3000), true},
		{"at max timestamp", time.UnixMilli(4999), true},
		{"at end (half-open)", time.UnixMilli(5000), false},
		{"after end", time.UnixMilli(5001), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.Contains(tc.timestamp); got != tc.want {
				t.Errorf("IntervalWindow.Contains(%v) = %v, want %v", tc.timestamp, got, tc.want)
			}
		})
	}
}

func TestIntervalWindow_Equals(t *testing.T) {
	w := IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)}

	tests := []struct {
		name       string
		other      BoundedWindow
		wantEquals bool
	}{
		{"identical interval", IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)}, true},
		{"different start", IntervalWindow{Start: time.UnixMilli(1001), End: time.UnixMilli(5000)}, false},
		{"different end", IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5001)}, false},
		{"different type (GlobalWindow)", GlobalWindow{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.Equals(tc.other); got != tc.wantEquals {
				t.Errorf("IntervalWindow.Equals(%v) = %v, want %v", tc.other, got, tc.wantEquals)
			}
		})
	}
}

func TestIntervalWindow_Span(t *testing.T) {
	tests := []struct {
		name     string
		w1       IntervalWindow
		w2       IntervalWindow
		wantSpan IntervalWindow
	}{
		{
			name:     "overlapping intervals",
			w1:       IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)},
			w2:       IntervalWindow{Start: time.UnixMilli(3000), End: time.UnixMilli(8000)},
			wantSpan: IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(8000)},
		},
		{
			name:     "disjoint intervals",
			w1:       IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(2000)},
			w2:       IntervalWindow{Start: time.UnixMilli(7000), End: time.UnixMilli(9000)},
			wantSpan: IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(9000)},
		},
		{
			name:     "contained interval",
			w1:       IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(10000)},
			w2:       IntervalWindow{Start: time.UnixMilli(3000), End: time.UnixMilli(6000)},
			wantSpan: IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(10000)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.w1.Span(tc.w2); !got.Equals(tc.wantSpan) {
				t.Errorf("IntervalWindow.Span(%v) = %v, want %v", tc.w2, got, tc.wantSpan)
			}
		})
	}
}

func TestIntervalWindow_Intersects(t *testing.T) {
	w := IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)}

	tests := []struct {
		name           string
		other          IntervalWindow
		wantIntersects bool
	}{
		{"overlapping right", IntervalWindow{Start: time.UnixMilli(3000), End: time.UnixMilli(8000)}, true},
		{"overlapping left", IntervalWindow{Start: time.UnixMilli(500), End: time.UnixMilli(2000)}, true},
		{"strictly inside", IntervalWindow{Start: time.UnixMilli(2000), End: time.UnixMilli(3000)}, true},
		{"touching at start boundary", IntervalWindow{Start: time.UnixMilli(0), End: time.UnixMilli(1000)}, false},
		{"touching at end boundary", IntervalWindow{Start: time.UnixMilli(5000), End: time.UnixMilli(6000)}, false},
		{"disjoint after", IntervalWindow{Start: time.UnixMilli(6000), End: time.UnixMilli(8000)}, false},
		{"disjoint before", IntervalWindow{Start: time.UnixMilli(100), End: time.UnixMilli(500)}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.Intersects(tc.other); got != tc.wantIntersects {
				t.Errorf("IntervalWindow.Intersects(%v) = %v, want %v", tc.other, got, tc.wantIntersects)
			}
		})
	}
}

func TestWindowCoders_Table(t *testing.T) {
	t.Run("GlobalWindowCoder", func(t *testing.T) {
		gwCoder := GlobalWindowCoder{}
		enc := coders.NewEncoder()
		gwCoder.Encode(enc, GlobalWindow{})
		if len(enc.Data()) != 0 {
			t.Errorf("GlobalWindow encoded to %d bytes, want 0", len(enc.Data()))
		}
		dec := coders.NewDecoder(enc.Data())
		decoded := gwCoder.Decode(dec)
		if !decoded.Equals(GlobalWindow{}) {
			t.Errorf("decoded GlobalWindow = %v, want GlobalWindow{}", decoded)
		}
	})

	t.Run("IntervalWindowCoder", func(t *testing.T) {
		iwCoder := IntervalWindowCoder{}
		tests := []struct {
			name   string
			window IntervalWindow
		}{
			{
				name:   "standard interval",
				window: IntervalWindow{Start: time.UnixMilli(1000), End: time.UnixMilli(5000)},
			},
			{
				name:   "large epoch interval",
				window: IntervalWindow{Start: time.UnixMilli(1454290000000), End: time.UnixMilli(1454293600000)},
			},
			{
				name:   "negative timestamp interval",
				window: IntervalWindow{Start: time.UnixMilli(-5000), End: time.UnixMilli(-1000)},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				enc := coders.NewEncoder()
				iwCoder.Encode(enc, tc.window)
				dec := coders.NewDecoder(enc.Data())
				decoded := iwCoder.Decode(dec)
				if !decoded.Equals(tc.window) {
					t.Errorf("roundtrip IntervalWindowCoder = %v, want %v", decoded, tc.window)
				}
			})
		}
	})
}
