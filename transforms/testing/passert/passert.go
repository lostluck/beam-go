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
	"fmt"
	"slices"
	"strings"

	"github.com/google/go-cmp/cmp"
	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/window"
)

type equalsFn[E beam.Element] struct {
	Side     beam.SideInputIter[E]
	Expected []E
}

func (fn *equalsFn[E]) ProcessBundle(dfc *beam.DFC[[]byte]) error {
	return dfc.Process(func(ec beam.ElmC, _ []byte) error {
		var got []E
		for v := range fn.Side.All(ec) {
			got = append(got, v)
		}
		want := slices.Clone(fn.Expected)
		slices.SortFunc(got, func(a, b E) int {
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		})
		slices.SortFunc(want, func(a, b E) int {
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		})
		if diff := cmp.Diff(want, got); diff != "" {
			return fmt.Errorf("passert.Equals failed (-want +got):\n%s", diff)
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
