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

// WindowOption configures aspects of a windowing strategy.
type WindowOption func(*Strategy)

// Trigger configures the trigger used for pane firings.
func Trigger(t trigger.Trigger) WindowOption {
	return func(s *Strategy) {
		s.Trigger = t
	}
}

// Accumulating sets the pane accumulation mode to ACCUMULATING (panes contain all prior data).
func Accumulating() WindowOption {
	return func(s *Strategy) {
		s.AccumulationMode = pipepb.AccumulationMode_ACCUMULATING
	}
}

// Discarding sets the pane accumulation mode to DISCARDING (panes contain only new data since last firing).
func Discarding() WindowOption {
	return func(s *Strategy) {
		s.AccumulationMode = pipepb.AccumulationMode_DISCARDING
	}
}

// AllowedLateness configures how long past the end of a window late data is accepted.
func AllowedLateness(d time.Duration) WindowOption {
	return func(s *Strategy) {
		s.AllowedLateness = d
	}
}

// OutputTime configures how the aggregate timestamp of an output pane is derived.
func OutputTime(ot pipepb.OutputTime_Enum) WindowOption {
	return func(s *Strategy) {
		s.OutputTime = ot
	}
}

// OutputTimeEndOfWindow sets the pane output timestamp to the end of the window.
func OutputTimeEndOfWindow() WindowOption {
	return OutputTime(pipepb.OutputTime_END_OF_WINDOW)
}

// OutputTimeEarliest sets the pane output timestamp to the earliest element timestamp in the pane.
func OutputTimeEarliest() WindowOption {
	return OutputTime(pipepb.OutputTime_EARLIEST_IN_PANE)
}

// OutputTimeLatest sets the pane output timestamp to the latest element timestamp in the pane.
func OutputTimeLatest() WindowOption {
	return OutputTime(pipepb.OutputTime_LATEST_IN_PANE)
}

// ClosingBehavior configures whether late data is emitted upon window expiration.
func ClosingBehavior(cb pipepb.ClosingBehavior_Enum) WindowOption {
	return func(s *Strategy) {
		s.ClosingBehavior = cb
	}
}

// OnTimeBehavior configures whether empty on-time panes are emitted.
func OnTimeBehavior(ob pipepb.OnTimeBehavior_Enum) WindowOption {
	return func(s *Strategy) {
		s.OnTimeBehavior = ob
	}
}
