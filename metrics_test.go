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
	"context"
	"testing"
)

func FuzzSamplerState(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint16(0))
	f.Add(uint8(1), uint16(1), uint16(1))
	f.Add(uint8(2), uint16(2), uint16(0))
	f.Add(uint8(1), uint16(3), uint16(23))
	f.Add(uint8(1), uint16(42), uint16(170))
	f.Add(uint8(2), uint16(16383), uint16(1<<15))

	f.Fuzz(func(t *testing.T, a uint8, b uint16, c uint16) {
		mets := newMetricsStore(0)
		phase := uint32(a % 3)
		transition := uint32(b % 16384)
		edge := uint32(c % 0xFFFF)

		mets.storeState(phase, transition, edge)
		cur := mets.curState()

		if got, want := cur.phase, uint32(phase); got != want {
			t.Errorf("incorrect state phase: got %v, want %v", got, want)
		}
		if got, want := cur.edge, edgeIndex(edge); got != want {
			t.Errorf("incorrect state edge: got %v, want %v", got, want)
		}
		if got, want := cur.transition, uint32(transition); got != want {
			t.Errorf("incorrect state transition: got %v, want %v", got, want)
		}
	})
}

type DistFn struct {
	Dist DistributionInt64
}

func (fn *DistFn) ProcessBundle(dfc *DFC[int]) error {
	return dfc.Process(func(ec ElmC, i int) error {
		fn.Dist.Update(dfc, int64(i))
		return nil
	})
}

func TestDistributionInt64(t *testing.T) {
	pipe, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		ParDo(s, src.Output, &DistFn{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	dist := pipe.Distributions["sink.Dist"]
	if got, want := dist.Count, int64(10); got != want {
		t.Errorf("incorrect state transition: got %v, want %v", got, want)
	}
	if got, want := dist.Sum, int64(45); got != want {
		t.Errorf("incorrect state transition: got %v, want %v", got, want)
	}
	if got, want := dist.Min, int64(0); got != want {
		t.Errorf("incorrect state transition: got %v, want %v", got, want)
	}
	if got, want := dist.Max, int64(9); got != want {
		t.Errorf("incorrect state transition: got %v, want %v", got, want)
	}
}
