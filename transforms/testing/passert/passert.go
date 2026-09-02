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

// Package passert provides assertions on PCollections for testing Beam pipelines.
//
// Assertions fail the pipeline execution immediately if the PCollection contents
// do not match the expected state.
package passert

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/google/go-cmp/cmp"
	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/window"
)

type equalsFn[E beam.Element] struct {
	Side     beam.SideInputIter[E]
	Expected []E
}

type encodedElement[E any] struct {
	val E
	raw []byte
}

func (fn *equalsFn[E]) ProcessBundle(dfc *beam.DFC[[]byte]) error {
	return dfc.Process(func(ec beam.ElmC, _ []byte) error {
		coder := fn.Side.Coder()
		var got []encodedElement[E]
		for v := range fn.Side.All(ec) {
			got = append(got, encodedElement[E]{
				val: v,
				raw: coders.Encode(coder, v),
			})
		}
		want := make([]encodedElement[E], len(fn.Expected))
		for i, v := range fn.Expected {
			want[i] = encodedElement[E]{
				val: v,
				raw: coders.Encode(coder, v),
			}
		}
		slices.SortFunc(got, func(a, b encodedElement[E]) int {
			return bytes.Compare(a.raw, b.raw)
		})
		slices.SortFunc(want, func(a, b encodedElement[E]) int {
			return bytes.Compare(a.raw, b.raw)
		})

		if len(got) != len(want) {
			gotVals := make([]E, len(got))
			for i, g := range got {
				gotVals[i] = g.val
			}
			wantVals := make([]E, len(want))
			for i, w := range want {
				wantVals[i] = w.val
			}
			return fmt.Errorf("passert.Equals failed (count mismatch: got %d, want %d):\n%s",
				len(got), len(want), cmp.Diff(wantVals, gotVals))
		}

		for i := range got {
			if !bytes.Equal(got[i].raw, want[i].raw) {
				gotVals := make([]E, len(got))
				for j, g := range got {
					gotVals[j] = g.val
				}
				wantVals := make([]E, len(want))
				for j, w := range want {
					wantVals[j] = w.val
				}
				diff := cmp.Diff(wantVals, gotVals)
				if diff == "" {
					return fmt.Errorf("passert.Equals failed (encoded bytes mismatch at index %d: got %x, want %x)",
						i, got[i].raw, want[i].raw)
				}
				return fmt.Errorf("passert.Equals failed (-want +got):\n%s", diff)
			}
		}
		return nil
	})
}

// Equals asserts that col contains exactly the expected elements (multiset comparison).
func Equals[E beam.Element](s *beam.Scope, col beam.PCol[E], expected ...E) {
	EqualsSlice(s, col, expected)
}

// EqualsSlice asserts that col contains exactly the elements in the expected slice.
func EqualsSlice[E beam.Element](s *beam.Scope, col beam.PCol[E], expected []E) {
	imp := s.Impulse()
	s.ParDo(imp, &equalsFn[E]{
		Side:     beam.AsSideIter(col),
		Expected: expected,
	}, beam.Name("passert.Equals"))
}

type countFn[E beam.Element] struct {
	Side          beam.SideInputIter[E]
	ExpectedCount int
}

func (fn *countFn[E]) ProcessBundle(dfc *beam.DFC[[]byte]) error {
	return dfc.Process(func(ec beam.ElmC, _ []byte) error {
		count := 0
		for range fn.Side.All(ec) {
			count++
		}
		if count != fn.ExpectedCount {
			return fmt.Errorf("passert.Count failed: got %d elements, want %d", count, fn.ExpectedCount)
		}
		return nil
	})
}

// Count asserts that col contains exactly expectedCount elements.
func Count[E beam.Element](s *beam.Scope, col beam.PCol[E], expectedCount int) {
	imp := s.Impulse()
	s.ParDo(imp, &countFn[E]{
		Side:          beam.AsSideIter(col),
		ExpectedCount: expectedCount,
	}, beam.Name("passert.Count"))
}

// Empty asserts that col contains zero elements.
func Empty[E beam.Element](s *beam.Scope, col beam.PCol[E]) {
	Count(s, col, 0)
}

type filterWindowFn[E beam.Element, W window.BoundedWindow] struct {
	Win    beam.ObserveWindow[W]
	Target W
	Out    beam.PCol[E]
}

func (fn *filterWindowFn[E, W]) ProcessBundle(dfc *beam.DFC[E]) error {
	return dfc.Process(func(ec beam.ElmC, elm E) error {
		w := fn.Win.Of(ec)
		if w.Equals(fn.Target) {
			fn.Out.Emit(ec, elm)
		}
		return nil
	})
}

// InWindow asserts that elements in col belonging to the specified window match expected.
func InWindow[E beam.Element, W window.BoundedWindow](s *beam.Scope, col beam.PCol[E], win W, expected ...E) {
	filtered := s.ParDo(col, &filterWindowFn[E, W]{Target: win}, beam.Name("passert.FilterWindow"))
	global := s.WindowInto(filtered.Out, window.GlobalWindows())
	EqualsSlice(s, global, expected)
}

type filterPaneFn[E beam.Element] struct {
	Pane   beam.ObservePane
	Target coders.PaneInfo
	Out    beam.PCol[E]
}

func (fn *filterPaneFn[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	return dfc.Process(func(ec beam.ElmC, elm E) error {
		p := fn.Pane.Of(ec)
		if p == fn.Target {
			fn.Out.Emit(ec, elm)
		}
		return nil
	})
}

// InPane asserts that elements in col belonging to the specified pane match expected.
func InPane[E beam.Element](s *beam.Scope, col beam.PCol[E], pane coders.PaneInfo, expected ...E) {
	filtered := s.ParDo(col, &filterPaneFn[E]{Target: pane}, beam.Name("passert.FilterPane"))
	global := s.WindowInto(filtered.Out, window.GlobalWindows())
	EqualsSlice(s, global, expected)
}
