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

// Package teststream provides a fluent builder for the TestStream runner primitive transform,
// used for simulating streaming sources with explicit event-time watermarks, timestamped elements,
// and processing-time advances.
package teststream

import (
	"time"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
)

// Event represents an event in a TestStream.
type Event[E beam.Element] interface {
	ToProto(coder coders.Coder[E]) (*pipepb.TestStreamPayload_Event, error)
}

type elementsEvent[E beam.Element] struct {
	Timestamp time.Time
	Elements  []E
}

func (ev *elementsEvent[E]) ToProto(coder coders.Coder[E]) (*pipepb.TestStreamPayload_Event, error) {
	var tsElements []*pipepb.TestStreamPayload_TimestampedElement
	for _, elm := range ev.Elements {
		enc := coders.NewEncoder()
		coder.Encode(enc, elm)
		tsElements = append(tsElements, &pipepb.TestStreamPayload_TimestampedElement{
			EncodedElement: append([]byte(nil), enc.Data()...),
			Timestamp:      ev.Timestamp.UnixMilli(),
		})
	}
	return &pipepb.TestStreamPayload_Event{
		Event: &pipepb.TestStreamPayload_Event_ElementEvent{
			ElementEvent: &pipepb.TestStreamPayload_Event_AddElements{
				Elements: tsElements,
			},
		},
	}, nil
}

type watermarkEvent[E beam.Element] struct {
	Watermark time.Time
}

func (ev *watermarkEvent[E]) ToProto(coder coders.Coder[E]) (*pipepb.TestStreamPayload_Event, error) {
	return &pipepb.TestStreamPayload_Event{
		Event: &pipepb.TestStreamPayload_Event_WatermarkEvent{
			WatermarkEvent: &pipepb.TestStreamPayload_Event_AdvanceWatermark{
				NewWatermark: ev.Watermark.UnixMilli(),
			},
		},
	}, nil
}

type procTimeEvent[E beam.Element] struct {
	Duration time.Duration
}

func (ev *procTimeEvent[E]) ToProto(coder coders.Coder[E]) (*pipepb.TestStreamPayload_Event, error) {
	return &pipepb.TestStreamPayload_Event{
		Event: &pipepb.TestStreamPayload_Event_ProcessingTimeEvent{
			ProcessingTimeEvent: &pipepb.TestStreamPayload_Event_AdvanceProcessingTime{
				AdvanceDuration: ev.Duration.Milliseconds(),
			},
		},
	}, nil
}

// Builder builds a TestStream for elements of type E.
type Builder[E beam.Element] struct {
	s      *beam.Scope
	events []Event[E]
}

// New creates a new TestStream builder attached to the given Scope.
func New[E beam.Element](s *beam.Scope) *Builder[E] {
	return &Builder[E]{s: s}
}

// AddElements appends an event that emits elements at the given event timestamp.
func (b *Builder[E]) AddElements(timestamp time.Time, elements ...E) *Builder[E] {
	b.events = append(b.events, &elementsEvent[E]{Timestamp: timestamp, Elements: elements})
	return b
}

// AdvanceWatermark appends an event advancing the watermark to the given timestamp.
func (b *Builder[E]) AdvanceWatermark(watermark time.Time) *Builder[E] {
	b.events = append(b.events, &watermarkEvent[E]{Watermark: watermark})
	return b
}

// AdvanceWatermarkToInfinity advances the watermark to the end of time.
func (b *Builder[E]) AdvanceWatermarkToInfinity() *Builder[E] {
	return b.AdvanceWatermark((window.GlobalWindow{}).MaxTimestamp())
}

// AdvanceProcessingTime advances the processing time clock by duration.
func (b *Builder[E]) AdvanceProcessingTime(duration time.Duration) *Builder[E] {
	b.events = append(b.events, &procTimeEvent[E]{Duration: duration})
	return b
}

// Events returns a slice containing the accumulated events.
func (b *Builder[E]) Events() []Event[E] {
	return append([]Event[E](nil), b.events...)
}

// Build materializes the TestStream runner transform in the pipeline graph, returning an unbounded PCol[E].
func (b *Builder[E]) Build() beam.PCol[E] {
	return beam.TestStream(b.s, func(coder coders.Coder[E], coderID string) (*pipepb.TestStreamPayload, error) {
		var protoEvents []*pipepb.TestStreamPayload_Event
		for _, ev := range b.events {
			pe, err := ev.ToProto(coder)
			if err != nil {
				return nil, err
			}
			protoEvents = append(protoEvents, pe)
		}
		return &pipepb.TestStreamPayload{
			CoderId: coderID,
			Events:  protoEvents,
		}, nil
	})
}
