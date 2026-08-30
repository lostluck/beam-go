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

// Package trigger provides a DSL for defining Beam triggers for windowed aggregations.
package trigger

import (
	"time"

	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// Trigger represents an Apache Beam trigger.
type Trigger interface {
	ToProto() *pipepb.Trigger
}

type defaultTrigger struct{}

func (defaultTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Default_{
			Default: &pipepb.Trigger_Default{},
		},
	}
}

// Default returns the default trigger (fires once when watermark passes the end of window).
func Default() Trigger {
	return defaultTrigger{}
}

type alwaysTrigger struct{}

func (alwaysTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Always_{
			Always: &pipepb.Trigger_Always{},
		},
	}
}

// Always returns a trigger that always fires on every element.
func Always() Trigger {
	return alwaysTrigger{}
}

type neverTrigger struct{}

func (neverTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Never_{
			Never: &pipepb.Trigger_Never{},
		},
	}
}

// Never returns a trigger that never fires.
func Never() Trigger {
	return neverTrigger{}
}

type elementCountTrigger struct {
	count int32
}

func (t elementCountTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_ElementCount_{
			ElementCount: &pipepb.Trigger_ElementCount{
				ElementCount: t.count,
			},
		},
	}
}

// AfterCount returns a trigger that fires after at least count elements have arrived in the pane.
func AfterCount(count int32) Trigger {
	return elementCountTrigger{count: count}
}

type repeatTrigger struct {
	sub Trigger
}

func (t repeatTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_Repeat_{
			Repeat: &pipepb.Trigger_Repeat{
				Subtrigger: t.sub.ToProto(),
			},
		},
	}
}

// Repeatedly returns a trigger that resets and repeats its subtrigger whenever it fires.
func Repeatedly(t Trigger) Trigger {
	return repeatTrigger{sub: t}
}

type afterAllTrigger struct {
	subtriggers []Trigger
}

func (t afterAllTrigger) ToProto() *pipepb.Trigger {
	subs := make([]*pipepb.Trigger, len(t.subtriggers))
	for i, sub := range t.subtriggers {
		subs[i] = sub.ToProto()
	}
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterAll_{
			AfterAll: &pipepb.Trigger_AfterAll{
				Subtriggers: subs,
			},
		},
	}
}

// AfterAll returns a trigger that fires after all of its subtriggers have fired.
func AfterAll(subtriggers ...Trigger) Trigger {
	return afterAllTrigger{subtriggers: subtriggers}
}

type afterAnyTrigger struct {
	subtriggers []Trigger
}

func (t afterAnyTrigger) ToProto() *pipepb.Trigger {
	subs := make([]*pipepb.Trigger, len(t.subtriggers))
	for i, sub := range t.subtriggers {
		subs[i] = sub.ToProto()
	}
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterAny_{
			AfterAny: &pipepb.Trigger_AfterAny{
				Subtriggers: subs,
			},
		},
	}
}

// AfterAny returns a trigger that fires after any of its subtriggers has fired.
func AfterAny(subtriggers ...Trigger) Trigger {
	return afterAnyTrigger{subtriggers: subtriggers}
}

type afterEachTrigger struct {
	subtriggers []Trigger
}

func (t afterEachTrigger) ToProto() *pipepb.Trigger {
	subs := make([]*pipepb.Trigger, len(t.subtriggers))
	for i, sub := range t.subtriggers {
		subs[i] = sub.ToProto()
	}
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterEach_{
			AfterEach: &pipepb.Trigger_AfterEach{
				Subtriggers: subs,
			},
		},
	}
}

// AfterEach returns a trigger that fires once for each subtrigger in sequence.
func AfterEach(subtriggers ...Trigger) Trigger {
	return afterEachTrigger{subtriggers: subtriggers}
}

type orFinallyTrigger struct {
	main    Trigger
	finally Trigger
}

func (t orFinallyTrigger) ToProto() *pipepb.Trigger {
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_OrFinally_{
			OrFinally: &pipepb.Trigger_OrFinally{
				Main:    t.main.ToProto(),
				Finally: t.finally.ToProto(),
			},
		},
	}
}

// OrFinally returns a trigger that fires according to main until finally fires, at which point it finishes.
func OrFinally(main, finally Trigger) Trigger {
	return orFinallyTrigger{main: main, finally: finally}
}

// WatermarkTriggerBuilder builds an AfterWatermark (EndOfWindow) trigger with early and late firings.
type WatermarkTriggerBuilder struct {
	early Trigger
	late  Trigger
}

// AfterWatermark returns a builder for an event-time watermark trigger.
func AfterWatermark() *WatermarkTriggerBuilder {
	return &WatermarkTriggerBuilder{}
}

// WithEarlyFirings specifies a trigger to fire speculatively before the watermark passes the end of window.
func (b *WatermarkTriggerBuilder) WithEarlyFirings(early Trigger) *WatermarkTriggerBuilder {
	b.early = early
	return b
}

// WithLateFirings specifies a trigger to fire after the watermark has passed the end of window.
func (b *WatermarkTriggerBuilder) WithLateFirings(late Trigger) *WatermarkTriggerBuilder {
	b.late = late
	return b
}

func (b *WatermarkTriggerBuilder) ToProto() *pipepb.Trigger {
	var earlyPb, latePb *pipepb.Trigger
	if b.early != nil {
		earlyPb = b.early.ToProto()
	}
	if b.late != nil {
		latePb = b.late.ToProto()
	}
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterEndOfWindow_{
			AfterEndOfWindow: &pipepb.Trigger_AfterEndOfWindow{
				EarlyFirings: earlyPb,
				LateFirings:  latePb,
			},
		},
	}
}

// ProcessingTimeTriggerBuilder builds an AfterProcessingTime trigger with delay transforms.
type ProcessingTimeTriggerBuilder struct {
	delay time.Duration
}

// AfterProcessingTime returns a builder for a processing-time trigger.
func AfterProcessingTime() *ProcessingTimeTriggerBuilder {
	return &ProcessingTimeTriggerBuilder{}
}

// PlusDelay adds a delay to the processing time firing.
func (b *ProcessingTimeTriggerBuilder) PlusDelay(d time.Duration) *ProcessingTimeTriggerBuilder {
	b.delay += d
	return b
}

func (b *ProcessingTimeTriggerBuilder) ToProto() *pipepb.Trigger {
	var transforms []*pipepb.TimestampTransform
	if b.delay > 0 {
		transforms = append(transforms, &pipepb.TimestampTransform{
			TimestampTransform: &pipepb.TimestampTransform_Delay_{
				Delay: &pipepb.TimestampTransform_Delay{
					DelayMillis: b.delay.Milliseconds(),
				},
			},
		})
	}
	return &pipepb.Trigger{
		Trigger: &pipepb.Trigger_AfterProcessingTime_{
			AfterProcessingTime: &pipepb.Trigger_AfterProcessingTime{
				TimestampTransforms: transforms,
			},
		},
	}
}
