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

package passert_test

import (
	"strings"
	"testing"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
	"lostluck.dev/beam-go/transforms/testing/passert"
	"lostluck.dev/beam-go/window"
)

func pipeName(t *testing.T) beamopts.Options {
	return beam.Name(t.Name())
}

type sourceFn[E beam.Element] struct {
	Elements []E
	Out      beam.PCol[E]
}

func (fn *sourceFn[E]) ProcessBundle(dfc *beam.DFC[[]byte]) error {
	return dfc.Process(func(ec beam.ElmC, _ []byte) error {
		for _, e := range fn.Elements {
			fn.Out.Emit(ec, e)
		}
		return nil
	})
}

func TestEquals_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[string]{Elements: []string{"apple", "banana", "cherry"}})
		// Assert in different order to ensure multiset equality
		passert.Equals(s, src.Out, "cherry", "apple", "banana")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}

func TestEquals_Failure(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[int]{Elements: []int{1, 2, 3}})
		passert.Equals(s, src.Out, 1, 2, 4)
		return nil
	}, pipeName(t))
	if err == nil {
		t.Fatal("expected pipeline to fail with assertion error, got nil")
	}
	if !strings.Contains(err.Error(), "passert.Equals failed") {
		t.Errorf("expected error to mention 'passert.Equals failed', got: %v", err)
	}
}

func TestEquals_KV_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[beam.KV[string, int]]{
			Elements: []beam.KV[string, int]{
				beam.Pair("k1", 10),
				beam.Pair("k2", 20),
				beam.Pair("k3", 30),
			},
		})
		passert.Equals(s, src.Out, beam.Pair("k2", 20), beam.Pair("k3", 30), beam.Pair("k1", 10))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}

func TestEquals_KV_Failure(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[beam.KV[string, int]]{
			Elements: []beam.KV[string, int]{
				beam.Pair("k1", 10),
				beam.Pair("k2", 20),
			},
		})
		passert.Equals(s, src.Out, beam.Pair("k1", 10), beam.Pair("k2", 999))
		return nil
	}, pipeName(t))
	if err == nil {
		t.Fatal("expected pipeline to fail with KV mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "passert.Equals failed") {
		t.Errorf("expected error to mention 'passert.Equals failed', got: %v", err)
	}
}

func TestCount_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[int]{Elements: []int{10, 20, 30, 40, 50}})
		passert.Count(s, src.Out, 5)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}

func TestCount_Failure(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[int]{Elements: []int{10, 20, 30}})
		passert.Count(s, src.Out, 5)
		return nil
	}, pipeName(t))
	if err == nil {
		t.Fatal("expected pipeline to fail with count mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "passert.Count failed") {
		t.Errorf("expected error to mention 'passert.Count failed', got: %v", err)
	}
}

func TestEmpty_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[int]{Elements: nil})
		passert.Empty(s, src.Out)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}

func TestEmpty_Failure(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[int]{Elements: []int{42}})
		passert.Empty(s, src.Out)
		return nil
	}, pipeName(t))
	if err == nil {
		t.Fatal("expected pipeline to fail with non-empty collection, got nil")
	}
	if !strings.Contains(err.Error(), "passert.Count failed") {
		t.Errorf("expected error to mention 'passert.Count failed', got: %v", err)
	}
}

func TestInWindow_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[string]{Elements: []string{"a", "b"}})
		passert.InWindow(s, src.Out, window.GlobalWindow{}, "a", "b")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}

func TestInWindow_Failure(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[string]{Elements: []string{"a", "b"}})
		// Intentionally expecting wrong elements in GlobalWindow
		passert.InWindow(s, src.Out, window.GlobalWindow{}, "a", "z")
		return nil
	}, pipeName(t))
	if err == nil {
		t.Fatal("expected pipeline to fail with window content mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "passert.Equals failed") {
		t.Errorf("expected error to mention 'passert.Equals failed', got: %v", err)
	}
}

func TestInPane_Success(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sourceFn[string]{Elements: []string{"x", "y"}})
		passert.InPane(s, src.Out, coders.NoFiringPane, "x", "y")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("expected pipeline to succeed, got error: %v", err)
	}
}
