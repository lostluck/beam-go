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
	"time"

	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window/trigger"
)

// Strategy defines how elements in a PCollection are windowed, triggered,
// and accumulated during aggregation transforms.
type Strategy struct {
	Fn               WindowFn
	Trigger          trigger.Trigger
	AccumulationMode pipepb.AccumulationMode_Enum
	AllowedLateness  time.Duration
	OutputTime       pipepb.OutputTime_Enum
	ClosingBehavior  pipepb.ClosingBehavior_Enum
	OnTimeBehavior   pipepb.OnTimeBehavior_Enum
}

// DefaultStrategy returns the default global windowing strategy with default triggering.
func DefaultStrategy() *Strategy {
	return &Strategy{
		Fn:               GlobalWindows(),
		Trigger:          trigger.Default(),
		AccumulationMode: pipepb.AccumulationMode_DISCARDING,
		AllowedLateness:  0,
		OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
		ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
		OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
	}
}

// NewStrategy creates a Windowing Strategy with the given WindowFn and options.
func NewStrategy(fn WindowFn, opts ...WindowOption) *Strategy {
	if fn == nil {
		fn = GlobalWindows()
	}
	strat := &Strategy{
		Fn:               fn,
		Trigger:          trigger.Default(),
		AccumulationMode: pipepb.AccumulationMode_DISCARDING,
		AllowedLateness:  0,
		OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
		ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
		OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(strat)
		}
	}
	return strat
}
