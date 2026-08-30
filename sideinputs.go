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
	"fmt"
	"iter"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	"lostluck.dev/beam-go/window"
)

type sideInputCommon struct {
	beamMixin

	valid  bool
	global nodeIndex
}

func (si *sideInputCommon) sideInput() nodeIndex {
	return si.global
}

type sideIface interface {
	sideInput() nodeIndex
	accessPatternUrn() string
	initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string)
}

type SideInputIter[E Element] struct {
	sideInputCommon

	initIterReader func(w []byte) harness.NextBuffer
}

func (*SideInputIter[E]) accessPatternUrn() string {
	return "beam:side_input:iterable:v1"
}

func (si *SideInputIter[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string) {
	si.initIterReader = func(w []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_IterableSideInput_{
				IterableSideInput: &fnpb.StateKey_IterableSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
				},
			},
		}
		// 50/50 on putting this on processor directly instead
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
}

func encodeWindow(w window.BoundedWindow) []byte {
	enc := coders.NewEncoder()
	switch win := w.(type) {
	case window.GlobalWindow:
		enc.GlobalWindow()
	case window.IntervalWindow:
		enc.IntervalWindow(win.End, win.Duration())
	default:
		enc.GlobalWindow()
	}
	return enc.Data()
}

func getActiveWindow(ec ElmC) window.BoundedWindow {
	if ec.window != nil {
		return ec.window
	}
	if len(ec.windows) > 0 {
		return ec.windows[0]
	}
	return window.GlobalWindow{}
}

var _ sideIface = &SideInputIter[int]{}

func (si *SideInputIter[E]) All(ec ElmC) iter.Seq[E] {
	r := si.initIterReader(encodeWindow(getActiveWindow(ec)))
	return iterClosure[E](r)
}

func validateSideInput[E any](emt PCol[E]) {
	if !emt.valid {
		panic("emitter is invalid")
	}
	var e E
	if isMetaType(e) {
		panic(fmt.Sprintf("type %T cannot be used as a side input value", e))
	}
}

// AsSideIter initializes an IterSideInput from a valid upstream Emitter.
// It allows access to the data of that Emitter's PCollection,
func AsSideIter[E Element](emt PCol[E]) SideInputIter[E] {
	validateSideInput(emt)
	return SideInputIter[E]{valid: true, global: emt.globalIndex}
}

// SideInputMap allows a side input to be accessed via multip-map key lookups.
type SideInputMap[K, V Element] struct {
	sideInputCommon

	initMapReader     func(w, k []byte) harness.NextBuffer
	initMapKeysReader func(w []byte) harness.NextBuffer
}

func (*SideInputMap[K, V]) accessPatternUrn() string {
	return "beam:side_input:multimap:v1"
}

func (si *SideInputMap[K, V]) initialize(ctx context.Context, dataCon harness.DataContext, url, sideID, transformID string) {
	si.initMapReader = func(w, k []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapSideInput_{
				MultimapSideInput: &fnpb.StateKey_MultimapSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
					Key:         k,
				},
			},
		}
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
	si.initMapKeysReader = func(w []byte) harness.NextBuffer {
		key := &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapKeysSideInput_{
				MultimapKeysSideInput: &fnpb.StateKey_MultimapKeysSideInput{
					TransformId: transformID,
					SideInputId: sideID,
					Window:      w,
				},
			},
		}
		// 50/50 on putting this on processor directly instead
		r, err := dataCon.State.OpenReader(ctx, url, key)
		if err != nil {
			panic(err)
		}
		return r
	}
}

var _ sideIface = &SideInputMap[int, int]{}

// Get looks up an iterator of values associated with the key.
func (si *SideInputMap[K, V]) Get(ec ElmC, k K) iter.Seq[V] {
	wData := encodeWindow(getActiveWindow(ec))
	// TODO cache coders in the side inputs?
	kc := MakeCoder[K]()
	encK := coders.NewEncoder()
	kc.Encode(encK, k)
	r := si.initMapReader(wData, encK.Data())
	return iterClosure[V](r)
}

// Keys looks up an iterator of keys available in the side input.
func (si *SideInputMap[K, V]) Keys(ec ElmC) iter.Seq[K] {
	wData := encodeWindow(getActiveWindow(ec))
	r := si.initMapKeysReader(wData)
	return iterClosure[K](r)
}

// AsSideMap initializes a MapSideInput from a valid upstream Emitter.
func AsSideMap[K, V Element](emt PCol[KV[K, V]]) SideInputMap[K, V] {
	validateSideInput(emt)
	return SideInputMap[K, V]{valid: true, global: emt.globalIndex}
}
