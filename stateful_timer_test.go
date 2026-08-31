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

package beam_test

import (
	"testing"
	"time"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/transforms/testing/passert"
	"lostluck.dev/beam-go/transforms/testing/teststream"
)

type EventTimeBatchingDoFn struct {
	Buffer     beam.StateBag[int]
	FlushTimer beam.TimerEvent[string]

	Output beam.PCol[beam.KV[string, int]]
}

func (fn *EventTimeBatchingDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, int]]) error {
	fn.FlushTimer.OnFire(dfc, func(ec beam.ElmC, key string) error {
		sum := 0
		for v := range fn.Buffer.Read(ec) {
			sum += v
		}
		fn.Buffer.Clear(ec)
		fn.Output.Emit(ec, beam.Pair(key, sum))
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, int]) error {
		fn.Buffer.Append(ec, elm.Value)
		fn.FlushTimer.Set(ec, ec.EventTime().Add(10*time.Second))
		return nil
	})
}

func TestStateful_EventTimeTimer(t *testing.T) {
	t0 := time.UnixMilli(0)

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, int]](s).
			AdvanceWatermark(t0).
			AddElements(t0.Add(1*time.Second), beam.Pair("k1", 10)).
			AddElements(t0.Add(2*time.Second), beam.Pair("k1", 20)).
			AddElements(t0.Add(1*time.Second), beam.Pair("k2", 5)).
			AdvanceWatermark(t0.Add(5 * time.Second)).  // Timer should NOT fire yet
			AdvanceWatermark(t0.Add(15 * time.Second)). // Timer FIRES!
			AdvanceWatermarkToInfinity().
			Build()

		batched := s.StatefulParDo(stream, &EventTimeBatchingDoFn{})
		passert.Equals(s, batched.Output, beam.Pair("k1", 30), beam.Pair("k2", 5))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type TaggedTimerDoFn struct {
	FastTimer beam.TimerEvent[string]
	SlowTimer beam.TimerEvent[string]

	Output beam.PCol[beam.KV[string, string]]
}

func (fn *TaggedTimerDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, string]]) error {
	fn.FastTimer.OnFireTagged(dfc, func(ec beam.ElmC, key string, tag string) error {
		fn.Output.Emit(ec, beam.Pair(key, "fast_"+tag))
		return nil
	})
	fn.SlowTimer.OnFireTagged(dfc, func(ec beam.ElmC, key string, tag string) error {
		fn.Output.Emit(ec, beam.Pair(key, "slow_"+tag))
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, string]) error {
		fn.FastTimer.SetWithTag(ec, elm.Value, ec.EventTime().Add(5*time.Second))
		fn.SlowTimer.SetWithTag(ec, elm.Value, ec.EventTime().Add(20*time.Second))
		return nil
	})
}

func TestStateful_DynamicTaggedTimers(t *testing.T) {
	t0 := time.UnixMilli(0)

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, string]](s).
			AdvanceWatermark(t0).
			AddElements(t0.Add(1*time.Second), beam.Pair("user1", "tagA")).
			AddElements(t0.Add(2*time.Second), beam.Pair("user1", "tagB")).
			AdvanceWatermark(t0.Add(10 * time.Second)). // Fast timers fire for tagA and tagB
			AdvanceWatermark(t0.Add(30 * time.Second)). // Slow timers fire for tagA and tagB
			AdvanceWatermarkToInfinity().
			Build()

		tagged := s.StatefulParDo(stream, &TaggedTimerDoFn{})
		passert.Equals(s, tagged.Output,
			beam.Pair("user1", "fast_tagA"),
			beam.Pair("user1", "fast_tagB"),
			beam.Pair("user1", "slow_tagA"),
			beam.Pair("user1", "slow_tagB"),
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type CancellableTimerDoFn struct {
	PendingTimer beam.TimerEvent[string]

	Output beam.PCol[beam.KV[string, string]]
}

func (fn *CancellableTimerDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, string]]) error {
	fn.PendingTimer.OnFire(dfc, func(ec beam.ElmC, key string) error {
		fn.Output.Emit(ec, beam.Pair(key, "fired"))
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, string]) error {
		switch elm.Value {
		case "set":
			fn.PendingTimer.Set(ec, ec.EventTime().Add(10*time.Second))
		case "cancel":
			fn.PendingTimer.Clear(ec)
		}
		return nil
	})
}

func TestStateful_TimerClear(t *testing.T) {
	t0 := time.UnixMilli(0)

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, string]](s).
			AdvanceWatermark(t0).
			AddElements(t0.Add(1*time.Second), beam.Pair("k1", "set")).
			AddElements(t0.Add(2*time.Second), beam.Pair("k1", "cancel")). // cancel k1 timer
			AddElements(t0.Add(1*time.Second), beam.Pair("k2", "set")).    // k2 timer NOT cancelled
			AdvanceWatermark(t0.Add(20 * time.Second)).                    // Only k2 fires
			AdvanceWatermarkToInfinity().
			Build()

		cancelled := s.StatefulParDo(stream, &CancellableTimerDoFn{})
		passert.Equals(s, cancelled.Output, beam.Pair("k2", "fired"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type ProcessingTimeDoFn struct {
	PTimer beam.TimerProcessing[string]

	Output beam.PCol[beam.KV[string, string]]
}

func (fn *ProcessingTimeDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, string]]) error {
	fn.PTimer.OnFire(dfc, func(ec beam.ElmC, key string) error {
		fn.Output.Emit(ec, beam.Pair(key, "pt_fired"))
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, string]) error {
		fn.PTimer.Set(ec, time.Now().Add(50*time.Millisecond))
		return nil
	})
}

func TestStateful_ProcessingTimeTimer(t *testing.T) {
	t0 := time.UnixMilli(0)

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, string]](s).
			AdvanceWatermark(t0).
			AddElements(t0, beam.Pair("k1", "ping")).
			AdvanceProcessingTime(200 * time.Millisecond).
			AdvanceWatermarkToInfinity().
			Build()

		pt := s.StatefulParDo(stream, &ProcessingTimeDoFn{})
		passert.Equals(s, pt.Output, beam.Pair("k1", "pt_fired"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type ExpiryFlushDoFn struct {
	Buffer   beam.StateBag[int]
	OnExpiry beam.OnWindowExpiration[string]

	Output beam.PCol[beam.KV[string, int]]
}

func (fn *ExpiryFlushDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, int]]) error {
	fn.OnExpiry.OnExpire(dfc, func(ec beam.ElmC, key string) error {
		sum := 0
		for v := range fn.Buffer.Read(ec) {
			sum += v
		}
		fn.Output.Emit(ec, beam.Pair(key, sum))
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, int]) error {
		fn.Buffer.Append(ec, elm.Value)
		return nil
	})
}

func TestStateful_OnWindowExpiration(t *testing.T) {
	t0 := time.UnixMilli(0)

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[beam.KV[string, int]](s).
			AdvanceWatermark(t0).
			AddElements(t0.Add(1*time.Second), beam.Pair("k1", 10)).
			AddElements(t0.Add(2*time.Second), beam.Pair("k1", 25)).
			AddElements(t0.Add(3*time.Second), beam.Pair("k2", 7)).
			AdvanceWatermarkToInfinity(). // Global window expires on watermark infinity
			Build()

		expired := s.StatefulParDo(stream, &ExpiryFlushDoFn{})
		passert.Equals(s, expired.Output, beam.Pair("k1", 35), beam.Pair("k2", 7))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}
