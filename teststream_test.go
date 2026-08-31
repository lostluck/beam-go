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

package beam

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
)

func TestTestStream_Translation_Primitives(t *testing.T) {
	s := &Scope{g: &graph{}}
	t0 := time.Unix(100, 0).UTC()

	pcol := TestStream(s, func(coder coders.Coder[string], coderID string) (*pipepb.TestStreamPayload, error) {
		enc1 := coders.NewEncoder()
		coder.Encode(enc1, "alpha")
		enc2 := coders.NewEncoder()
		coder.Encode(enc2, "beta")

		return &pipepb.TestStreamPayload{
			CoderId: coderID,
			Events: []*pipepb.TestStreamPayload_Event{
				{
					Event: &pipepb.TestStreamPayload_Event_ElementEvent{
						ElementEvent: &pipepb.TestStreamPayload_Event_AddElements{
							Elements: []*pipepb.TestStreamPayload_TimestampedElement{
								{EncodedElement: enc1.Data(), Timestamp: t0.UnixMilli()},
								{EncodedElement: enc2.Data(), Timestamp: t0.UnixMilli()},
							},
						},
					},
				},
				{
					Event: &pipepb.TestStreamPayload_Event_WatermarkEvent{
						WatermarkEvent: &pipepb.TestStreamPayload_Event_AdvanceWatermark{
							NewWatermark: t0.Add(5 * time.Second).UnixMilli(),
						},
					},
				},
				{
					Event: &pipepb.TestStreamPayload_Event_ProcessingTimeEvent{
						ProcessingTimeEvent: &pipepb.TestStreamPayload_Event_AdvanceProcessingTime{
							AdvanceDuration: (2 * time.Second).Milliseconds(),
						},
					},
				},
				{
					Event: &pipepb.TestStreamPayload_Event_WatermarkEvent{
						WatermarkEvent: &pipepb.TestStreamPayload_Event_AdvanceWatermark{
							NewWatermark: (window.GlobalWindow{}).MaxTimestamp().UnixMilli(),
						},
					},
				},
			},
		}, nil
	})

	if !pcol.valid {
		t.Fatalf("expected valid PCol from TestStream")
	}

	pipe := s.g.marshal(map[string]reflect.Type{})
	comps := pipe.GetComponents()

	// Verify output PCollection is UNBOUNDED
	outPCol := comps.GetPcollections()["n0"]
	if outPCol == nil {
		t.Fatalf("expected pcollection n0 in components")
	}
	if outPCol.GetIsBounded() != pipepb.IsBounded_UNBOUNDED {
		t.Errorf("expected IsBounded UNBOUNDED, got %v", outPCol.GetIsBounded())
	}

	// Verify transform spec and environment
	pt := comps.GetTransforms()["e0"]
	if pt == nil {
		t.Fatalf("expected transform e0 in components")
	}
	if got, want := pt.GetSpec().GetUrn(), "beam:transform:teststream:v1"; got != want {
		t.Errorf("expected URN %q, got %q", want, got)
	}
	if pt.GetEnvironmentId() != "" {
		t.Errorf("expected empty EnvironmentId for runner primitive, got %q", pt.GetEnvironmentId())
	}

	// Unmarshal and verify TestStreamPayload
	var payload pipepb.TestStreamPayload
	if err := proto.Unmarshal(pt.GetSpec().GetPayload(), &payload); err != nil {
		t.Fatalf("failed to unmarshal TestStreamPayload: %v", err)
	}

	if payload.GetCoderId() != outPCol.GetCoderId() {
		t.Errorf("expected payload CoderId %q to match PCollection CoderId %q", payload.GetCoderId(), outPCol.GetCoderId())
	}

	if len(payload.GetEvents()) != 4 {
		t.Fatalf("expected 4 events, got %d", len(payload.GetEvents()))
	}

	// Event 0: AddElements
	ev0 := payload.GetEvents()[0].GetElementEvent()
	if ev0 == nil || len(ev0.GetElements()) != 2 {
		t.Fatalf("expected 2 elements in event 0, got %v", ev0)
	}
	strCoder := coderFromProto[string](comps.Coders, outPCol.GetCoderId())
	var decodedElements []string
	for _, el := range ev0.GetElements() {
		if el.GetTimestamp() != t0.UnixMilli() {
			t.Errorf("expected timestamp %d, got %d", t0.UnixMilli(), el.GetTimestamp())
		}
		dec := coders.NewDecoder(el.GetEncodedElement())
		decodedElements = append(decodedElements, strCoder.Decode(dec))
	}
	if diff := cmp.Diff([]string{"alpha", "beta"}, decodedElements); diff != "" {
		t.Errorf("decoded elements mismatch (-want +got):\n%s", diff)
	}

	// Event 1: AdvanceWatermark
	ev1 := payload.GetEvents()[1].GetWatermarkEvent()
	if ev1 == nil || ev1.GetNewWatermark() != t0.Add(5*time.Second).UnixMilli() {
		t.Errorf("expected watermark %d, got %v", t0.Add(5*time.Second).UnixMilli(), ev1)
	}

	// Event 2: AdvanceProcessingTime
	ev2 := payload.GetEvents()[2].GetProcessingTimeEvent()
	if ev2 == nil || ev2.GetAdvanceDuration() != (2*time.Second).Milliseconds() {
		t.Errorf("expected processing duration 2000ms, got %v", ev2)
	}

	// Event 3: AdvanceWatermarkToInfinity
	ev3 := payload.GetEvents()[3].GetWatermarkEvent()
	if ev3 == nil || ev3.GetNewWatermark() != (window.GlobalWindow{}).MaxTimestamp().UnixMilli() {
		t.Errorf("expected watermark %d, got %v", (window.GlobalWindow{}).MaxTimestamp().UnixMilli(), ev3)
	}
}

func TestTestStream_Translation_KV(t *testing.T) {
	s := &Scope{g: &graph{}}
	t0 := time.Unix(200, 0).UTC()

	pcol := TestStream(s, func(coder coders.Coder[KV[string, int]], coderID string) (*pipepb.TestStreamPayload, error) {
		enc1 := coders.NewEncoder()
		coder.Encode(enc1, Pair("sensorA", 42))
		enc2 := coders.NewEncoder()
		coder.Encode(enc2, Pair("sensorB", 99))

		return &pipepb.TestStreamPayload{
			CoderId: coderID,
			Events: []*pipepb.TestStreamPayload_Event{
				{
					Event: &pipepb.TestStreamPayload_Event_ElementEvent{
						ElementEvent: &pipepb.TestStreamPayload_Event_AddElements{
							Elements: []*pipepb.TestStreamPayload_TimestampedElement{
								{EncodedElement: enc1.Data(), Timestamp: t0.UnixMilli()},
								{EncodedElement: enc2.Data(), Timestamp: t0.UnixMilli()},
							},
						},
					},
				},
				{
					Event: &pipepb.TestStreamPayload_Event_WatermarkEvent{
						WatermarkEvent: &pipepb.TestStreamPayload_Event_AdvanceWatermark{
							NewWatermark: t0.Add(10 * time.Second).UnixMilli(),
						},
					},
				},
			},
		}, nil
	})

	if !pcol.valid {
		t.Fatalf("expected valid PCol from TestStream")
	}

	pipe := s.g.marshal(map[string]reflect.Type{})
	comps := pipe.GetComponents()

	// Verify output PCollection is UNBOUNDED and has KV coder
	outPCol := comps.GetPcollections()["n0"]
	if outPCol == nil {
		t.Fatalf("expected pcollection n0 in components")
	}
	if outPCol.GetIsBounded() != pipepb.IsBounded_UNBOUNDED {
		t.Errorf("expected IsBounded UNBOUNDED, got %v", outPCol.GetIsBounded())
	}

	coderProto := comps.GetCoders()[outPCol.GetCoderId()]
	if coderProto == nil || coderProto.GetSpec().GetUrn() != "beam:coder:kv:v1" {
		t.Fatalf("expected KV coder for n0, got %v", coderProto)
	}

	pt := comps.GetTransforms()["e0"]
	if pt == nil {
		t.Fatalf("expected transform e0 in components")
	}
	if got, want := pt.GetSpec().GetUrn(), "beam:transform:teststream:v1"; got != want {
		t.Errorf("expected URN %q, got %q", want, got)
	}

	var payload pipepb.TestStreamPayload
	if err := proto.Unmarshal(pt.GetSpec().GetPayload(), &payload); err != nil {
		t.Fatalf("failed to unmarshal TestStreamPayload: %v", err)
	}

	if payload.GetCoderId() != outPCol.GetCoderId() {
		t.Errorf("expected payload CoderId %q to match PCollection CoderId %q", payload.GetCoderId(), outPCol.GetCoderId())
	}

	ev0 := payload.GetEvents()[0].GetElementEvent()
	if ev0 == nil || len(ev0.GetElements()) != 2 {
		t.Fatalf("expected 2 KV elements in event 0, got %v", ev0)
	}

	kvCoder := coderFromProto[KV[string, int]](comps.Coders, outPCol.GetCoderId())
	var decodedKVs []KV[string, int]
	for _, el := range ev0.GetElements() {
		if el.GetTimestamp() != t0.UnixMilli() {
			t.Errorf("expected timestamp %d, got %d", t0.UnixMilli(), el.GetTimestamp())
		}
		dec := coders.NewDecoder(el.GetEncodedElement())
		decodedKVs = append(decodedKVs, kvCoder.Decode(dec))
	}

	wantKVs := []KV[string, int]{Pair("sensorA", 42), Pair("sensorB", 99)}
	if diff := cmp.Diff(wantKVs, decodedKVs); diff != "" {
		t.Errorf("decoded KV elements mismatch (-want +got):\n%s", diff)
	}
}
