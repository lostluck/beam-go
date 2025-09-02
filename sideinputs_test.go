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

// convenience function to allow the discard type to be inferred.
func namedDiscard[E Element](s *Scope, input PCol[E], name string) {
	ParDo(s, input, &DiscardFn[E]{}, Name(name))
}

func TestSideInputIter(t *testing.T) {
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		onlySide := ParDo(s, imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
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
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		kvsrc := ParDo(s, src.Output, &KeyMod[int]{Mod: 3})
		onlySide := ParDo(s, imp, &OnlySideMap[int, int]{Side: AsSideMap(kvsrc.Output)})
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
