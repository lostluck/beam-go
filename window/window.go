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

// Package window provides event-time window representations, windowing strategies,
// and window assignment functions for Apache Beam pipelines.
package window

import (
	"time"

	"lostluck.dev/beam-go/coders"
)

// Standard Beam timestamp bounds (in milliseconds).
const (
	MinTimestampMillis int64 = -9223372036854775
	MaxTimestampMillis int64 = 9223372036854775
)

var (
	// MinTimestamp is the earliest representable Beam event time.
	MinTimestamp = time.UnixMilli(MinTimestampMillis)
	// MaxTimestamp is the latest representable Beam event time.
	MaxTimestamp = time.UnixMilli(MaxTimestampMillis)
	// GlobalWindowMaxTimestamp is the end-of-window timestamp for the GlobalWindow.
	GlobalWindowMaxTimestamp = time.UnixMilli(MaxTimestampMillis - 1000)
)

// BoundedWindow represents an event-time window in Apache Beam.
type BoundedWindow interface {
	// MaxTimestamp returns the inclusive upper bound of event time for this window.
	MaxTimestamp() time.Time
	// Equals returns true if two windows are identical.
	Equals(other BoundedWindow) bool
}

// GlobalWindow is the default singleton window spanning all of time [-infinity, +infinity).
type GlobalWindow struct{}

var _ BoundedWindow = GlobalWindow{}

// MaxTimestamp returns the end-of-time timestamp for the global window.
func (GlobalWindow) MaxTimestamp() time.Time {
	return GlobalWindowMaxTimestamp
}

// Equals returns true if other is a GlobalWindow.
func (GlobalWindow) Equals(other BoundedWindow) bool {
	_, ok := other.(GlobalWindow)
	return ok
}

func (GlobalWindow) String() string {
	return "GlobalWindow"
}

// IntervalWindow represents a half-open event-time interval [Start, End).
type IntervalWindow struct {
	Start time.Time
	End   time.Time
}

var _ BoundedWindow = IntervalWindow{}

// MaxTimestamp returns the inclusive maximum event-time timestamp of this interval (End - 1ms).
func (w IntervalWindow) MaxTimestamp() time.Time {
	return w.End.Add(-1 * time.Millisecond)
}

// Duration returns the length of time covered by this interval.
func (w IntervalWindow) Duration() time.Duration {
	return w.End.Sub(w.Start)
}

// Equals returns true if both start and end timestamps match.
func (w IntervalWindow) Equals(other BoundedWindow) bool {
	if o, ok := other.(IntervalWindow); ok {
		return w.Start.Equal(o.Start) && w.End.Equal(o.End)
	}
	return false
}

// Contains returns true if the timestamp t falls within [Start, End).
func (w IntervalWindow) Contains(t time.Time) bool {
	return (w.Start.Before(t) || w.Start.Equal(t)) && t.Before(w.End)
}

// Span returns the minimal interval window covering both this window and other.
func (w IntervalWindow) Span(other IntervalWindow) IntervalWindow {
	start := w.Start
	if other.Start.Before(start) {
		start = other.Start
	}
	end := w.End
	if other.End.After(end) {
		end = other.End
	}
	return IntervalWindow{Start: start, End: end}
}

// Intersects returns true if this interval overlaps with other.
func (w IntervalWindow) Intersects(other IntervalWindow) bool {
	return !w.Start.After(other.End) && !other.Start.After(w.End) && !w.End.Equal(other.Start) && !other.End.Equal(w.Start)
}

func (w IntervalWindow) String() string {
	return "[" + w.Start.UTC().Format(time.RFC3339Nano) + ", " + w.End.UTC().Format(time.RFC3339Nano) + ")"
}

// Encode encodes the GlobalWindow using the Beam standard global window coder (0 bytes).
func (GlobalWindow) Encode(enc *coders.Encoder) {
	enc.GlobalWindow()
}

// Decode decodes the GlobalWindow using the Beam standard global window coder.
func (*GlobalWindow) Decode(dec *coders.Decoder) {
	dec.GlobalWindow()
}

// Encode encodes the IntervalWindow using the Beam standard interval window coder (end millis + duration varint).
func (w IntervalWindow) Encode(enc *coders.Encoder) {
	enc.IntervalWindow(w.End, w.Duration())
}

// Decode decodes the IntervalWindow using the Beam standard interval window coder.
func (w *IntervalWindow) Decode(dec *coders.Decoder) {
	end, dur := dec.IntervalWindow()
	w.End = end
	w.Start = end.Add(-dur)
}

// GlobalWindowCoder implements coders.Coder[GlobalWindow].
type GlobalWindowCoder struct{}

var _ coders.Coder[GlobalWindow] = GlobalWindowCoder{}

func (GlobalWindowCoder) Encode(enc *coders.Encoder, w GlobalWindow) {
	w.Encode(enc)
}

func (GlobalWindowCoder) Decode(dec *coders.Decoder) GlobalWindow {
	var w GlobalWindow
	w.Decode(dec)
	return w
}

// IntervalWindowCoder implements coders.Coder[IntervalWindow].
type IntervalWindowCoder struct{}

var _ coders.Coder[IntervalWindow] = IntervalWindowCoder{}

func (IntervalWindowCoder) Encode(enc *coders.Encoder, w IntervalWindow) {
	w.Encode(enc)
}

func (IntervalWindowCoder) Decode(dec *coders.Decoder) IntervalWindow {
	var w IntervalWindow
	w.Decode(dec)
	return w
}
