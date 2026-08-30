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

package trigger

import (
	"testing"
	"time"

	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

func TestTriggers_ToProto(t *testing.T) {
	tests := []struct {
		name      string
		trigger   Trigger
		checkFunc func(t *testing.T, pb *pipepb.Trigger)
	}{
		{
			name:    "Default",
			trigger: Default(),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				if pb.GetDefault() == nil {
					t.Errorf("expected Default trigger, got %v", pb)
				}
			},
		},
		{
			name:    "Always",
			trigger: Always(),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				if pb.GetAlways() == nil {
					t.Errorf("expected Always trigger, got %v", pb)
				}
			},
		},
		{
			name:    "Never",
			trigger: Never(),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				if pb.GetNever() == nil {
					t.Errorf("expected Never trigger, got %v", pb)
				}
			},
		},
		{
			name:    "AfterCount",
			trigger: AfterCount(100),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				ec := pb.GetElementCount()
				if ec == nil || ec.GetElementCount() != 100 {
					t.Errorf("expected ElementCount=100, got %v", ec)
				}
			},
		},
		{
			name:    "Repeatedly",
			trigger: Repeatedly(AfterCount(5)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				rep := pb.GetRepeat()
				if rep == nil || rep.GetSubtrigger().GetElementCount().GetElementCount() != 5 {
					t.Errorf("expected Repeat(ElementCount=5), got %v", rep)
				}
			},
		},
		{
			name:    "AfterAll",
			trigger: AfterAll(AfterCount(10), AfterCount(20)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				aa := pb.GetAfterAll()
				if aa == nil || len(aa.GetSubtriggers()) != 2 {
					t.Errorf("expected AfterAll with 2 subs, got %v", aa)
				}
			},
		},
		{
			name:    "AfterAny",
			trigger: AfterAny(AfterCount(10), AfterCount(20)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				aa := pb.GetAfterAny()
				if aa == nil || len(aa.GetSubtriggers()) != 2 {
					t.Errorf("expected AfterAny with 2 subs, got %v", aa)
				}
			},
		},
		{
			name:    "AfterEach",
			trigger: AfterEach(AfterCount(10), AfterCount(20)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				ae := pb.GetAfterEach()
				if ae == nil || len(ae.GetSubtriggers()) != 2 {
					t.Errorf("expected AfterEach with 2 subs, got %v", ae)
				}
			},
		},
		{
			name:    "OrFinally",
			trigger: OrFinally(AfterCount(10), AfterCount(50)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				of := pb.GetOrFinally()
				if of == nil || of.GetMain().GetElementCount().GetElementCount() != 10 || of.GetFinally().GetElementCount().GetElementCount() != 50 {
					t.Errorf("expected OrFinally(10, 50), got %v", of)
				}
			},
		},
		{
			name: "AfterWatermark_WithEarlyAndLate",
			trigger: AfterWatermark().
				WithEarlyFirings(AfterCount(1)).
				WithLateFirings(AfterCount(2)),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				eow := pb.GetAfterEndOfWindow()
				if eow == nil {
					t.Fatalf("expected AfterEndOfWindow, got %v", pb)
				}
				if eow.GetEarlyFirings().GetElementCount().GetElementCount() != 1 {
					t.Errorf("expected early trigger ElementCount=1, got %v", eow.GetEarlyFirings())
				}
				if eow.GetLateFirings().GetElementCount().GetElementCount() != 2 {
					t.Errorf("expected late trigger ElementCount=2, got %v", eow.GetLateFirings())
				}
			},
		},
		{
			name:    "AfterProcessingTime_WithDelay",
			trigger: AfterProcessingTime().PlusDelay(5 * time.Minute),
			checkFunc: func(t *testing.T, pb *pipepb.Trigger) {
				apt := pb.GetAfterProcessingTime()
				if apt == nil || len(apt.GetTimestampTransforms()) != 1 {
					t.Fatalf("expected AfterProcessingTime with 1 transform, got %v", apt)
				}
				delay := apt.GetTimestampTransforms()[0].GetDelay().GetDelayMillis()
				if delay != (5 * time.Minute).Milliseconds() {
					t.Errorf("expected delay 300000ms, got %dms", delay)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pb := tc.trigger.ToProto()
			tc.checkFunc(t, pb)
		})
	}
}
