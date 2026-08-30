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
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// WindowFn assigns elements to windows and provides windowing metadata.
type WindowFn interface {
	// AssignWindows returns the windows for an element with the given timestamp.
	AssignWindows(t time.Time) []BoundedWindow
	// WindowCoderURN returns the URN of the coder used for the assigned windows.
	WindowCoderURN() string
	// MergeStatus indicates whether windows of this type can merge.
	MergeStatus() pipepb.MergeStatus_Enum
	// AssignsToOneWindow returns true if elements are always assigned to exactly one window.
	AssignsToOneWindow() bool
	// ToProto returns the Beam runner API FunctionSpec for this window fn.
	ToProto() *pipepb.FunctionSpec
}

type globalWindowsFn struct{}

// GlobalWindows assigns all elements to the singleton GlobalWindow.
func GlobalWindows() WindowFn {
	return globalWindowsFn{}
}

func (globalWindowsFn) AssignWindows(t time.Time) []BoundedWindow {
	return []BoundedWindow{GlobalWindow{}}
}

func (globalWindowsFn) WindowCoderURN() string {
	return "beam:coder:global_window:v1"
}

func (globalWindowsFn) MergeStatus() pipepb.MergeStatus_Enum {
	return pipepb.MergeStatus_NON_MERGING
}

func (globalWindowsFn) AssignsToOneWindow() bool {
	return true
}

func (globalWindowsFn) ToProto() *pipepb.FunctionSpec {
	payload, _ := proto.Marshal(&pipepb.GlobalWindowsPayload{})
	return &pipepb.FunctionSpec{
		Urn:     "beam:window_fn:global_windows:v1",
		Payload: payload,
	}
}

type fixedWindowsFn struct {
	size   time.Duration
	offset time.Duration
}

// FixedWindows assigns elements to non-overlapping fixed-duration interval windows.
// Optional offset shifts the window boundaries.
func FixedWindows(size time.Duration, offsets ...time.Duration) WindowFn {
	if size <= 0 {
		panic(fmt.Sprintf("FixedWindows size must be positive, got %v", size))
	}
	var offset time.Duration
	if len(offsets) > 0 {
		offset = offsets[0]
	}
	return fixedWindowsFn{size: size, offset: offset}
}

func (fn fixedWindowsFn) AssignWindows(t time.Time) []BoundedWindow {
	tMillis := t.UnixMilli()
	sizeMillis := fn.size.Milliseconds()
	offsetMillis := fn.offset.Milliseconds()

	rem := (tMillis - offsetMillis) % sizeMillis
	if rem < 0 {
		rem += sizeMillis
	}
	startMillis := tMillis - rem
	endMillis := startMillis + sizeMillis

	return []BoundedWindow{
		IntervalWindow{
			Start: time.UnixMilli(startMillis),
			End:   time.UnixMilli(endMillis),
		},
	}
}

func (fixedWindowsFn) WindowCoderURN() string {
	return "beam:coder:interval_window:v1"
}

func (fixedWindowsFn) MergeStatus() pipepb.MergeStatus_Enum {
	return pipepb.MergeStatus_NON_MERGING
}

func (fixedWindowsFn) AssignsToOneWindow() bool {
	return true
}

func (fn fixedWindowsFn) ToProto() *pipepb.FunctionSpec {
	payload, _ := proto.Marshal(&pipepb.FixedWindowsPayload{
		Size:   durationpb.New(fn.size),
		Offset: timestamppb.New(time.UnixMilli(fn.offset.Milliseconds())),
	})
	return &pipepb.FunctionSpec{
		Urn:     "beam:window_fn:fixed_windows:v1",
		Payload: payload,
	}
}

type slidingWindowsFn struct {
	period time.Duration
	every  time.Duration
	offset time.Duration
}

// SlidingWindows assigns elements to overlapping fixed-duration interval windows.
// period specifies the window size; every specifies the slide period.
// Optional offset shifts window boundaries.
func SlidingWindows(period, every time.Duration, offsets ...time.Duration) WindowFn {
	if period <= 0 {
		panic(fmt.Sprintf("SlidingWindows period must be positive, got %v", period))
	}
	if every <= 0 {
		panic(fmt.Sprintf("SlidingWindows every must be positive, got %v", every))
	}
	var offset time.Duration
	if len(offsets) > 0 {
		offset = offsets[0]
	}
	return slidingWindowsFn{period: period, every: every, offset: offset}
}

func (fn slidingWindowsFn) AssignWindows(t time.Time) []BoundedWindow {
	tMillis := t.UnixMilli()
	periodMillis := fn.period.Milliseconds()
	everyMillis := fn.every.Milliseconds()
	offsetMillis := fn.offset.Milliseconds()

	rem := (tMillis - offsetMillis) % everyMillis
	if rem < 0 {
		rem += everyMillis
	}
	latestStart := tMillis - rem

	var windows []BoundedWindow
	for start := latestStart; start+periodMillis > tMillis; start -= everyMillis {
		windows = append(windows, IntervalWindow{
			Start: time.UnixMilli(start),
			End:   time.UnixMilli(start + periodMillis),
		})
	}
	return windows
}

func (slidingWindowsFn) WindowCoderURN() string {
	return "beam:coder:interval_window:v1"
}

func (slidingWindowsFn) MergeStatus() pipepb.MergeStatus_Enum {
	return pipepb.MergeStatus_NON_MERGING
}

func (fn slidingWindowsFn) AssignsToOneWindow() bool {
	return fn.period <= fn.every
}

func (fn slidingWindowsFn) ToProto() *pipepb.FunctionSpec {
	payload, _ := proto.Marshal(&pipepb.SlidingWindowsPayload{
		Size:   durationpb.New(fn.period),
		Period: durationpb.New(fn.every),
		Offset: timestamppb.New(time.UnixMilli(fn.offset.Milliseconds())),
	})
	return &pipepb.FunctionSpec{
		Urn:     "beam:window_fn:sliding_windows:v1",
		Payload: payload,
	}
}

type sessionWindowsFn struct {
	gap time.Duration
}

// Sessions assigns elements to merging session windows based on activity gap.
func Sessions(gap time.Duration) WindowFn {
	if gap <= 0 {
		panic(fmt.Sprintf("Sessions gap must be positive, got %v", gap))
	}
	return sessionWindowsFn{gap: gap}
}

func (fn sessionWindowsFn) AssignWindows(t time.Time) []BoundedWindow {
	return []BoundedWindow{
		IntervalWindow{
			Start: t,
			End:   t.Add(fn.gap),
		},
	}
}

func (sessionWindowsFn) WindowCoderURN() string {
	return "beam:coder:interval_window:v1"
}

func (sessionWindowsFn) MergeStatus() pipepb.MergeStatus_Enum {
	return pipepb.MergeStatus_NEEDS_MERGE
}

func (sessionWindowsFn) AssignsToOneWindow() bool {
	return true
}

func (fn sessionWindowsFn) ToProto() *pipepb.FunctionSpec {
	payload, _ := proto.Marshal(&pipepb.SessionWindowsPayload{
		GapSize: durationpb.New(fn.gap),
	})
	return &pipepb.FunctionSpec{
		Urn:     "beam:window_fn:session_windows:v1",
		Payload: payload,
	}
}
