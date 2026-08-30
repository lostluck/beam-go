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

	"google.golang.org/protobuf/proto"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
)

// convenience function to allow the discard type to be inferred.
func namedDiscard[E Element](s *Scope, input PCol[E], name string) {
	s.ParDo(input, &DiscardFn[E]{}, Name(name))
}

func TestSideInputIter(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: 10})
		onlySide := s.ParDo(imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
		namedDiscard(s, onlySide.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
}

func TestSideInputMap(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: 10})
		kvsrc := s.ParDo(src.Output, &KeyMod[int]{Mod: 3})
		onlySide := s.ParDo(imp, &OnlySideMap[int, int]{Side: AsSideMap(kvsrc.Output)})
		namedDiscard(s, onlySide.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
}

type OnlySideIter[E Element] struct {
	Side SideInputIter[E]

	Out PCol[E]
}

func (fn *OnlySideIter[E]) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, elm []byte) error {
		for elm := range fn.Side.All(ec) {
			fn.Out.Emit(ec, elm)
		}
		return nil
	})
}

type OnlySideMap[K, V Element] struct {
	Side SideInputMap[K, V]

	Out PCol[KV[K, V]]
}

func (fn *OnlySideMap[K, V]) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, elm []byte) error {
		for key := range fn.Side.Keys(ec) {
			for val := range fn.Side.Get(ec, key) {
				fn.Out.Emit(ec, KV[K, V]{key, val})
			}
		}
		return nil
	})
}

func TestSideInput_WindowMappingFn_Table(t *testing.T) {
	tests := []struct {
		name                 string
		buildPipeline        func(s *Scope)
		wantWindowMappingUrn string
	}{
		{
			name: "GlobalWindow_SideInput",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: 5})
				_ = s.ParDo(imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
			},
			wantWindowMappingUrn: "beam:window_mapping_fn:global:v1",
		},
		{
			name: "FixedWindows_SideInput",
			buildPipeline: func(s *Scope) {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: 5})
				winSrc := s.WindowInto(src.Output, window.FixedWindows(10*time.Second))
				_ = s.ParDo(imp, &OnlySideIter[int]{Side: AsSideIter(winSrc)})
			},
			wantWindowMappingUrn: "beam:window_mapping_fn:interval:v1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scope{g: &graph{}}
			tc.buildPipeline(s)

			pipe := s.g.marshal(map[string]reflect.Type{
				"lostluck.dev/beam-go.SourceFn":           reflect.TypeFor[SourceFn](),
				"lostluck.dev/beam-go.OnlySideIter[int]": reflect.TypeFor[OnlySideIter[int]](),
			})
			comps := pipe.GetComponents()

			var foundSideInput bool
			for _, pt := range comps.GetTransforms() {
				if pt.GetSpec().GetUrn() == "beam:transform:pardo:v1" {
					var payload pipepb.ParDoPayload
					if err := proto.Unmarshal(pt.GetSpec().GetPayload(), &payload); err == nil && len(payload.GetSideInputs()) > 0 {
						for _, si := range payload.GetSideInputs() {
							foundSideInput = true
							if si.GetWindowMappingFn().GetUrn() != tc.wantWindowMappingUrn {
								t.Errorf("WindowMappingFn URN = %q, want %q", si.GetWindowMappingFn().GetUrn(), tc.wantWindowMappingUrn)
							}
						}
					}
				}
			}
			if !foundSideInput {
				t.Fatalf("no side input found in translated pipeline transforms")
			}
		})
	}
}
