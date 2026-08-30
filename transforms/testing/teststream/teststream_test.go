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

package teststream_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/transforms/testing/teststream"
	"lostluck.dev/beam-go/window"
)

func TestTestStream_Builder_Chaining(t *testing.T) {
	t0 := time.Unix(100, 0).UTC()
	b := teststream.New[string](nil)

	if got := b.AddElements(t0, "a"); got != b {
		t.Errorf("AddElements did not return same builder for chaining")
	}
	if got := b.AdvanceWatermark(t0.Add(5 * time.Second)); got != b {
		t.Errorf("AdvanceWatermark did not return same builder for chaining")
	}
	if got := b.AdvanceProcessingTime(2 * time.Second); got != b {
		t.Errorf("AdvanceProcessingTime did not return same builder for chaining")
	}
	if got := b.AdvanceWatermarkToInfinity(); got != b {
		t.Errorf("AdvanceWatermarkToInfinity did not return same builder for chaining")
	}

	if len(b.Events()) != 4 {
		t.Fatalf("expected 4 events, got %d", len(b.Events()))
	}
}

func TestTestStream_Builder_StringEvents(t *testing.T) {
	t0 := time.Unix(500, 0).UTC()
	b := teststream.New[string](nil).
		AddElements(t0, "apple", "banana").
		AdvanceWatermark(t0.Add(10 * time.Second)).
		AdvanceProcessingTime(3 * time.Second).
		AdvanceWatermarkToInfinity()

	events := b.Events()
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	strCoder := coders.MakeCoder[string]()

	// Event 0: AddElements
	pe0, err := events[0].ToProto(strCoder)
	if err != nil {
		t.Fatalf("failed ToProto for event 0: %v", err)
	}
	elms0 := pe0.GetElementEvent().GetElements()
	if len(elms0) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elms0))
	}
	var decoded []string
	for _, el := range elms0 {
		if el.GetTimestamp() != t0.UnixMilli() {
			t.Errorf("expected timestamp %d, got %d", t0.UnixMilli(), el.GetTimestamp())
		}
		dec := coders.NewDecoder(el.GetEncodedElement())
		decoded = append(decoded, strCoder.Decode(dec))
	}
	if diff := cmp.Diff([]string{"apple", "banana"}, decoded); diff != "" {
		t.Errorf("decoded elements mismatch (-want +got):\n%s", diff)
	}

	// Event 1: AdvanceWatermark
	pe1, err := events[1].ToProto(strCoder)
	if err != nil {
		t.Fatalf("failed ToProto for event 1: %v", err)
	}
	wm1 := pe1.GetWatermarkEvent().GetNewWatermark()
	if want := t0.Add(10 * time.Second).UnixMilli(); wm1 != want {
		t.Errorf("expected watermark %d, got %d", want, wm1)
	}

	// Event 2: AdvanceProcessingTime
	pe2, err := events[2].ToProto(strCoder)
	if err != nil {
		t.Fatalf("failed ToProto for event 2: %v", err)
	}
	dur2 := pe2.GetProcessingTimeEvent().GetAdvanceDuration()
	if want := (3 * time.Second).Milliseconds(); dur2 != want {
		t.Errorf("expected duration %d, got %d", want, dur2)
	}

	// Event 3: AdvanceWatermarkToInfinity
	pe3, err := events[3].ToProto(strCoder)
	if err != nil {
		t.Fatalf("failed ToProto for event 3: %v", err)
	}
	wm3 := pe3.GetWatermarkEvent().GetNewWatermark()
	if want := (window.GlobalWindow{}).MaxTimestamp().UnixMilli(); wm3 != want {
		t.Errorf("expected infinity watermark %d, got %d", want, wm3)
	}
}

type testKVCoder struct {
	kCoder coders.Coder[string]
	vCoder coders.Coder[int]
}

func (c testKVCoder) Encode(enc *coders.Encoder, kv beam.KV[string, int]) {
	c.kCoder.Encode(enc, kv.Key)
	c.vCoder.Encode(enc, kv.Value)
}

func (c testKVCoder) Decode(dec *coders.Decoder) beam.KV[string, int] {
	return beam.Pair(c.kCoder.Decode(dec), c.vCoder.Decode(dec))
}

func TestTestStream_Builder_KV(t *testing.T) {
	t0 := time.Unix(700, 0).UTC()
	b := teststream.New[beam.KV[string, int]](nil).
		AddElements(t0, beam.Pair("sensor1", 100), beam.Pair("sensor2", 200)).
		AdvanceWatermark(t0.Add(20 * time.Second)).
		AdvanceWatermarkToInfinity()

	events := b.Events()
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	kvCoder := testKVCoder{
		kCoder: coders.MakeCoder[string](),
		vCoder: coders.MakeCoder[int](),
	}

	// Event 0: AddElements
	pe0, err := events[0].ToProto(kvCoder)
	if err != nil {
		t.Fatalf("failed ToProto for KV event 0: %v", err)
	}
	elms0 := pe0.GetElementEvent().GetElements()
	if len(elms0) != 2 {
		t.Fatalf("expected 2 KV elements, got %d", len(elms0))
	}
	var decodedKVs []beam.KV[string, int]
	for _, el := range elms0 {
		if el.GetTimestamp() != t0.UnixMilli() {
			t.Errorf("expected timestamp %d, got %d", t0.UnixMilli(), el.GetTimestamp())
		}
		dec := coders.NewDecoder(el.GetEncodedElement())
		decodedKVs = append(decodedKVs, kvCoder.Decode(dec))
	}
	wantKVs := []beam.KV[string, int]{
		beam.Pair("sensor1", 100),
		beam.Pair("sensor2", 200),
	}
	if diff := cmp.Diff(wantKVs, decodedKVs); diff != "" {
		t.Errorf("decoded KV elements mismatch (-want +got):\n%s", diff)
	}

	// Event 1: AdvanceWatermark
	pe1, err := events[1].ToProto(kvCoder)
	if err != nil {
		t.Fatalf("failed ToProto for KV event 1: %v", err)
	}
	wm1 := pe1.GetWatermarkEvent().GetNewWatermark()
	if want := t0.Add(20 * time.Second).UnixMilli(); wm1 != want {
		t.Errorf("expected watermark %d, got %d", want, wm1)
	}

	// Event 2: AdvanceWatermarkToInfinity
	pe2, err := events[2].ToProto(kvCoder)
	if err != nil {
		t.Fatalf("failed ToProto for KV event 2: %v", err)
	}
	wm2 := pe2.GetWatermarkEvent().GetNewWatermark()
	if want := (window.GlobalWindow{}).MaxTimestamp().UnixMilli(); wm2 != want {
		t.Errorf("expected infinity watermark %d, got %d", want, wm2)
	}
}
