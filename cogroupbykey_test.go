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
	"fmt"
	"testing"
	"time"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/transforms/testing/passert"
	"lostluck.dev/beam-go/transforms/testing/teststream"
	"lostluck.dev/beam-go/window"
)

type FormatCoGBK2Fn struct {
	Output beam.PCol[string]
}

func (fn *FormatCoGBK2Fn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult2[string, int]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult2[string, int]]) error {
		var names []string
		for name := range elm.Value.Val1.All() {
			names = append(names, name)
		}
		var scores []int
		for score := range elm.Value.Val2.All() {
			scores = append(scores, score)
		}
		fn.Output.Emit(ec, fmt.Sprintf("%s: names=%v scores=%v", elm.Key, names, scores))
		return nil
	})
}

func TestCoGBK2_Basic(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		names := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k1", "Alicia"),
			beam.Pair("k2", "Bob"),
			beam.Pair("k3", "Charlie"),
		)
		scores := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k2", 200),
			beam.Pair("k2", 250),
			beam.Pair("k4", 400),
		)

		grouped := s.CoGBK2(names, scores)
		formatted := s.ParDo(grouped, &FormatCoGBK2Fn{})
		passert.Equals(s, formatted.Output,
			"k1: names=[Alice Alicia] scores=[100]",
			"k2: names=[Bob] scores=[200 250]",
			"k3: names=[Charlie] scores=[]",
			"k4: names=[] scores=[400]",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type FormatCoGBK3Fn struct {
	Output beam.PCol[string]
}

func (fn *FormatCoGBK3Fn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult3[string, int, bool]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult3[string, int, bool]]) error {
		var s1 []string
		for v := range elm.Value.Val1.All() {
			s1 = append(s1, v)
		}
		var s2 []int
		for v := range elm.Value.Val2.All() {
			s2 = append(s2, v)
		}
		var s3 []bool
		for v := range elm.Value.Val3.All() {
			s3 = append(s3, v)
		}
		fn.Output.Emit(ec, fmt.Sprintf("%s: s1=%v s2=%v s3=%v", elm.Key, s1, s2, s3))
		return nil
	})
}

func TestCoGBK3_Basic(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		col1 := s.Create(
			beam.Pair("k1", "A"),
			beam.Pair("k2", "B"),
		)
		col2 := s.Create(
			beam.Pair("k1", 10),
			beam.Pair("k3", 30),
		)
		col3 := s.Create(
			beam.Pair("k1", true),
			beam.Pair("k2", false),
		)

		grouped := s.CoGBK3(col1, col2, col3)
		formatted := s.ParDo(grouped, &FormatCoGBK3Fn{})
		passert.Equals(s, formatted.Output,
			"k1: s1=[A] s2=[10] s3=[true]",
			"k2: s1=[B] s2=[] s3=[false]",
			"k3: s1=[] s2=[30] s3=[]",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type FormatTaggedCoGBKFn struct {
	TagUsers  beam.Tag[string]
	TagOrders beam.Tag[int]
	TagActive beam.Tag[bool]

	Output beam.PCol[string]
}

func (fn *FormatTaggedCoGBKFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult]) error {
		var users []string
		for u := range elm.Value.Read(fn.TagUsers).All() {
			users = append(users, u)
		}
		var orders []int
		for o := range elm.Value.Read(fn.TagOrders).All() {
			orders = append(orders, o)
		}
		var actives []bool
		for a := range elm.Value.Read(fn.TagActive).All() {
			actives = append(actives, a)
		}
		fn.Output.Emit(ec, fmt.Sprintf("%s: users=%v orders=%v active=%v", elm.Key, users, orders, actives))
		return nil
	})
}

func TestCoGBK_Tags_Basic(t *testing.T) {
	tagUsers := beam.NewTag[string]("users")
	tagOrders := beam.NewTag[int]("orders")
	tagActive := beam.NewTag[bool]("active")

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		users := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k2", "Bob"),
		)
		orders := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k1", 200),
			beam.Pair("k3", 500),
		)
		active := s.Create(
			beam.Pair("k1", true),
			beam.Pair("k2", false),
		)

		grouped := beam.CoGBK(s,
			beam.BindTag(tagUsers, users),
			beam.BindTag(tagOrders, orders),
			beam.BindTag(tagActive, active),
		)

		formatted := s.ParDo(grouped, &FormatTaggedCoGBKFn{
			TagUsers:  tagUsers,
			TagOrders: tagOrders,
			TagActive: tagActive,
		})

		passert.Equals(s, formatted.Output,
			"k1: users=[Alice] orders=[100 200] active=[true]",
			"k2: users=[Bob] orders=[] active=[false]",
			"k3: users=[] orders=[500] active=[]",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type testWindowedEvent struct {
	Key   string
	Name  string
	Score int
}

func TestCoGBK2_Windowed(t *testing.T) {
	t0 := time.UnixMilli(0)
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		stream := teststream.New[testWindowedEvent](s).
			AdvanceWatermark(t0).
			AddElements(t0.Add(1*time.Second), testWindowedEvent{Key: "k1", Name: "Alice", Score: 100}).
			AddElements(t0.Add(12*time.Second), testWindowedEvent{Key: "k1", Name: "Alicia", Score: 200}).
			AdvanceWatermarkToInfinity().
			Build()

		names := s.Map(stream, func(e testWindowedEvent) beam.KV[string, string] {
			return beam.Pair(e.Key, e.Name)
		})
		scores := s.Map(stream, func(e testWindowedEvent) beam.KV[string, int] {
			return beam.Pair(e.Key, e.Score)
		})

		winN := s.WindowInto(names, window.FixedWindows(10*time.Second))
		winS := s.WindowInto(scores, window.FixedWindows(10*time.Second))

		grouped := s.CoGBK2(winN, winS)
		formatted := s.ParDo(grouped, &FormatCoGBK2Fn{})
		globalOut := s.WindowInto(formatted.Output, window.GlobalWindows())
		passert.Equals(s, globalOut,
			"k1: names=[Alice] scores=[100]",
			"k1: names=[Alicia] scores=[200]",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type CoGBK2SumFn struct {
	Output beam.PCol[int]
}

func (fn *CoGBK2SumFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult2[int, int]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult2[int, int]]) error {
		sum := 0
		for v1 := range elm.Value.Val1.All() {
			sum += v1
		}
		for v2 := range elm.Value.Val2.All() {
			sum += v2
		}
		fn.Output.Emit(ec, sum)
		return nil
	})
}

func TestCoGBK2_Sums(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		c1 := s.Create(beam.Pair("k", 1), beam.Pair("k", 2))
		c2 := s.Create(beam.Pair("k", 3), beam.Pair("k", 4))
		g := s.CoGBK2(c1, c2)
		sums := s.ParDo(g, &CoGBK2SumFn{})
		passert.Equals(s, sums.Output, 10)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type CoGBK3SumFn struct {
	Output beam.PCol[int]
}

func (fn *CoGBK3SumFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult3[int, int, int]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult3[int, int, int]]) error {
		sum := 0
		for v1 := range elm.Value.Val1.All() {
			sum += v1
		}
		for v2 := range elm.Value.Val2.All() {
			sum += v2
		}
		for v3 := range elm.Value.Val3.All() {
			sum += v3
		}
		fn.Output.Emit(ec, sum)
		return nil
	})
}

func TestCoGBK3_Sums(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		c1 := s.Create(beam.Pair("k", 1))
		c2 := s.Create(beam.Pair("k", 2))
		c3 := s.Create(beam.Pair("k", 3))
		g := s.CoGBK3(c1, c2, c3)
		sums := s.ParDo(g, &CoGBK3SumFn{})
		passert.Equals(s, sums.Output, 6)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type Tags2SumFn struct {
	Tag1 beam.Tag[int]
	Tag2 beam.Tag[int]

	Output beam.PCol[int]
}

func (fn *Tags2SumFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult]) error {
		sum := 0
		for v1 := range elm.Value.Read(fn.Tag1).All() {
			sum += v1
		}
		for v2 := range elm.Value.Read(fn.Tag2).All() {
			sum += v2
		}
		fn.Output.Emit(ec, sum)
		return nil
	})
}

type Tags3SumFn struct {
	Tag1 beam.Tag[int]
	Tag2 beam.Tag[int]
	Tag3 beam.Tag[int]

	Output beam.PCol[int]
}

func (fn *Tags3SumFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult]) error {
		sum := 0
		for v1 := range elm.Value.Read(fn.Tag1).All() {
			sum += v1
		}
		for v2 := range elm.Value.Read(fn.Tag2).All() {
			sum += v2
		}
		for v3 := range elm.Value.Read(fn.Tag3).All() {
			sum += v3
		}
		fn.Output.Emit(ec, sum)
		return nil
	})
}

func TestCoGBK_Tags_Sums(t *testing.T) {
	tag1 := beam.NewTag[int]("t1")
	tag2 := beam.NewTag[int]("t2")
	tag3 := beam.NewTag[int]("t3")

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		c1 := s.Create(beam.Pair("k", 1))
		c2 := s.Create(beam.Pair("k", 2))
		c3 := s.Create(beam.Pair("k", 3))
		g := beam.CoGBK(s, beam.BindTag(tag1, c1), beam.BindTag(tag2, c2), beam.BindTag(tag3, c3))
		sums := s.ParDo(g, &Tags3SumFn{Tag1: tag1, Tag2: tag2, Tag3: tag3})
		passert.Equals(s, sums.Output, 6)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

type MultipleIterInspectFn struct {
	TagNums beam.Tag[int]

	Output beam.PCol[string]
}

func (fn *MultipleIterInspectFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.CoGBKResult]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.CoGBKResult]) error {
		// First pass: sum
		sum := 0
		for n := range elm.Value.Read(fn.TagNums).All() {
			sum += n
		}
		// Second pass: product
		prod := 1
		for n := range elm.Value.Read(fn.TagNums).All() {
			prod *= n
		}
		fn.Output.Emit(ec, fmt.Sprintf("%s: sum=%d prod=%d", elm.Key, sum, prod))
		return nil
	})
}

func TestCoGBK_Tags_MultipleIter(t *testing.T) {
	tagNums := beam.NewTag[int]("nums")

	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		c := s.Create(
			beam.Pair("k1", 2),
			beam.Pair("k1", 3),
			beam.Pair("k1", 4),
		)
		g := beam.CoGBK(s, beam.BindTag(tagNums, c))
		formatted := s.ParDo(g, &MultipleIterInspectFn{TagNums: tagNums})
		passert.Equals(s, formatted.Output, "k1: sum=9 prod=24")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

func BenchmarkCoGBK2_vs_Tags2(b *testing.B) {
	tag1 := beam.NewTag[int]("t1")
	tag2 := beam.NewTag[int]("t2")

	b.Run("CoGBK2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = beam.LaunchAndWait(b.Context(), func(s *beam.Scope) error {
				c1 := s.Create(beam.Pair("k", 1), beam.Pair("k", 2))
				c2 := s.Create(beam.Pair("k", 3), beam.Pair("k", 4))
				g := s.CoGBK2(c1, c2)
				s.ParDo(g, &CoGBK2SumFn{})
				return nil
			}, pipeName(b))
		}
	})

	b.Run("Tags2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = beam.LaunchAndWait(b.Context(), func(s *beam.Scope) error {
				c1 := s.Create(beam.Pair("k", 1), beam.Pair("k", 2))
				c2 := s.Create(beam.Pair("k", 3), beam.Pair("k", 4))
				g := beam.CoGBK(s, beam.BindTag(tag1, c1), beam.BindTag(tag2, c2))
				s.ParDo(g, &Tags2SumFn{Tag1: tag1, Tag2: tag2})
				return nil
			}, pipeName(b))
		}
	})
}

func BenchmarkCoGBK3_vs_Tags3(b *testing.B) {
	tag1 := beam.NewTag[int]("t1")
	tag2 := beam.NewTag[int]("t2")
	tag3 := beam.NewTag[int]("t3")

	b.Run("CoGBK3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = beam.LaunchAndWait(b.Context(), func(s *beam.Scope) error {
				c1 := s.Create(beam.Pair("k", 1))
				c2 := s.Create(beam.Pair("k", 2))
				c3 := s.Create(beam.Pair("k", 3))
				g := s.CoGBK3(c1, c2, c3)
				s.ParDo(g, &CoGBK3SumFn{})
				return nil
			}, pipeName(b))
		}
	})

	b.Run("Tags3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = beam.LaunchAndWait(b.Context(), func(s *beam.Scope) error {
				c1 := s.Create(beam.Pair("k", 1))
				c2 := s.Create(beam.Pair("k", 2))
				c3 := s.Create(beam.Pair("k", 3))
				g := beam.CoGBK(s, beam.BindTag(tag1, c1), beam.BindTag(tag2, c2), beam.BindTag(tag3, c3))
				s.ParDo(g, &Tags3SumFn{Tag1: tag1, Tag2: tag2, Tag3: tag3})
				return nil
			}, pipeName(b))
		}
	})
}
