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
	"fmt"
	"sync/atomic"

	"github.com/go-json-experiment/json"
	"lostluck.dev/beam-go/internal/beamopts"
)

// CoGBKResult2 represents grouped values for 2 input collections.
type CoGBKResult2[V1, V2 Element] struct {
	Val1 Iter[V1]
	Val2 Iter[V2]
}

// CoGBKResult3 represents grouped values for 3 input collections.
type CoGBKResult3[V1, V2, V3 Element] struct {
	Val1 Iter[V1]
	Val2 Iter[V2]
	Val3 Iter[V3]
}

// CoGBKResult represents multi-input grouped values for open-ended CoGBK groupings.
type CoGBKResult struct {
	rawBuffers map[int][][]byte
}

var globalTagCounter atomic.Int64

// Tag is a typed identifier for a specific input PCollection in an open-ended CoGBK.
type Tag[V Element] struct {
	id   int
	name string
}

// NewTag creates a typed tag for binding and reading values in CoGBK.
func NewTag[V Element](name string) Tag[V] {
	id := int(globalTagCounter.Add(1))
	return Tag[V]{
		id:   id,
		name: name,
	}
}

// MarshalJSON serializes the tag ID.
func (t Tag[V]) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "%d", t.id), nil
}

// UnmarshalJSON deserializes the tag ID.
func (t *Tag[V]) UnmarshalJSON(data []byte) error {
	var id int
	if err := json.Unmarshal(data, &id, json.DefaultOptionsV2()); err != nil {
		return err
	}
	t.id = id
	return nil
}

// Read extracts an iterator of typed elements from the CoGBKResult for the given tag.
func (res CoGBKResult) Read[V Element](tag Tag[V]) Iter[V] {
	rawSlice, ok := res.rawBuffers[tag.id]
	if !ok || len(rawSlice) == 0 {
		return sliceIter([]V(nil))
	}
	values := make([]V, len(rawSlice))
	for i, raw := range rawSlice {
		var val V
		if err := json.Unmarshal(raw, &val, json.DefaultOptionsV2()); err != nil {
			panic(fmt.Sprintf("failed to unmarshal tag %q (id %d): %v", tag.name, tag.id, err))
		}
		values[i] = val
	}
	return sliceIter(values)
}

// TaggedPCol associates a typed Tag with an input PCollection for CoGBK.
type TaggedPCol[K Keys] struct {
	tagID int
	bind  func(s *Scope) PCol[KV[K, taggedRawBytes]]
}

// BindTag binds a typed Tag to an input PCollection of KV pairs.
func BindTag[K Keys, V Element](tag Tag[V], col PCol[KV[K, V]]) TaggedPCol[K] {
	return TaggedPCol[K]{
		tagID: tag.id,
		bind: func(s *Scope) PCol[KV[K, taggedRawBytes]] {
			dofn := s.ParDo(col, &tagRawBytesFn[K, V]{TagID: tag.id})
			return dofn.Output
		},
	}
}

// CoGBK groups elements from multiple tagged input collections by key.
func CoGBK[K Keys](s *Scope, taggedCols ...TaggedPCol[K]) PCol[KV[K, CoGBKResult]] {
	return coGBKInternal(s, taggedCols, nil)
}

func coGBKInternal[K Keys](s *Scope, taggedCols []TaggedPCol[K], opts []beamopts.Options) PCol[KV[K, CoGBKResult]] {
	if len(taggedCols) == 0 {
		panic("CoGBK requires at least one tagged collection")
	}

	taggedPCols := make([]PCol[KV[K, taggedRawBytes]], len(taggedCols))
	for i, tc := range taggedCols {
		taggedPCols[i] = tc.bind(s)
	}

	var flattened PCol[KV[K, taggedRawBytes]]
	if len(taggedPCols) == 1 {
		flattened = taggedPCols[0]
	} else {
		flattened = s.Flatten(taggedPCols...)
	}

	grouped := s.GBK(flattened, opts...)
	untagged := s.ParDo(grouped, &untagRawBytesFn[K]{})
	return untagged.Output
}

// CoGBK2 groups 2 PCollections by key into CoGBKResult2.
func (s *Scope) CoGBK2[K Keys, V1, V2 Element](
	col1 PCol[KV[K, V1]],
	col2 PCol[KV[K, V2]],
	opts ...beamopts.Options,
) PCol[KV[K, CoGBKResult2[V1, V2]]] {
	tag1 := NewTag[V1]("v1")
	tag2 := NewTag[V2]("v2")
	res := coGBKInternal(s, []TaggedPCol[K]{BindTag(tag1, col1), BindTag(tag2, col2)}, opts)
	untagged := s.ParDo(res, &untagCoGBK2Fn[K, V1, V2]{Tag1: tag1, Tag2: tag2})
	return untagged.Output
}

// CoGBK3 groups 3 PCollections by key into CoGBKResult3.
func (s *Scope) CoGBK3[K Keys, V1, V2, V3 Element](
	col1 PCol[KV[K, V1]],
	col2 PCol[KV[K, V2]],
	col3 PCol[KV[K, V3]],
	opts ...beamopts.Options,
) PCol[KV[K, CoGBKResult3[V1, V2, V3]]] {
	tag1 := NewTag[V1]("v1")
	tag2 := NewTag[V2]("v2")
	tag3 := NewTag[V3]("v3")
	res := coGBKInternal(s, []TaggedPCol[K]{BindTag(tag1, col1), BindTag(tag2, col2), BindTag(tag3, col3)}, opts)
	untagged := s.ParDo(res, &untagCoGBK3Fn[K, V1, V2, V3]{Tag1: tag1, Tag2: tag2, Tag3: tag3})
	return untagged.Output
}

// --- Internal Implementation Details for CoGBK2 & CoGBK3 ---

type untagCoGBK2Fn[K Keys, V1, V2 Element] struct {
	Tag1 Tag[V1]
	Tag2 Tag[V2]

	Output PCol[KV[K, CoGBKResult2[V1, V2]]]
}

func (fn *untagCoGBK2Fn[K, V1, V2]) ProcessBundle(dfc *DFC[KV[K, CoGBKResult]]) error {
	return dfc.Process(func(ec ElmC, elm KV[K, CoGBKResult]) error {
		fn.Output.Emit(ec, Pair(elm.Key, CoGBKResult2[V1, V2]{
			Val1: elm.Value.Read(fn.Tag1),
			Val2: elm.Value.Read(fn.Tag2),
		}))
		return nil
	})
}

type untagCoGBK3Fn[K Keys, V1, V2, V3 Element] struct {
	Tag1 Tag[V1]
	Tag2 Tag[V2]
	Tag3 Tag[V3]

	Output PCol[KV[K, CoGBKResult3[V1, V2, V3]]]
}

func (fn *untagCoGBK3Fn[K, V1, V2, V3]) ProcessBundle(dfc *DFC[KV[K, CoGBKResult]]) error {
	return dfc.Process(func(ec ElmC, elm KV[K, CoGBKResult]) error {
		fn.Output.Emit(ec, Pair(elm.Key, CoGBKResult3[V1, V2, V3]{
			Val1: elm.Value.Read(fn.Tag1),
			Val2: elm.Value.Read(fn.Tag2),
			Val3: elm.Value.Read(fn.Tag3),
		}))
		return nil
	})
}

// --- Internal Implementation Details for Open-Ended Tag[V] CoGBK ---

type taggedRawBytes struct {
	Tag     int
	Payload []byte
}

type tagRawBytesFn[K Keys, V Element] struct {
	TagID  int
	Output PCol[KV[K, taggedRawBytes]]
}

func (fn *tagRawBytesFn[K, V]) ProcessBundle(dfc *DFC[KV[K, V]]) error {
	return dfc.Process(func(ec ElmC, elm KV[K, V]) error {
		bytes, err := json.Marshal(elm.Value, json.DefaultOptionsV2())
		if err != nil {
			return err
		}
		fn.Output.Emit(ec, Pair(elm.Key, taggedRawBytes{
			Tag:     fn.TagID,
			Payload: bytes,
		}))
		return nil
	})
}

type untagRawBytesFn[K Keys] struct {
	Output PCol[KV[K, CoGBKResult]]
}

func (fn *untagRawBytesFn[K]) ProcessBundle(dfc *DFC[KV[K, Iter[taggedRawBytes]]]) error {
	return dfc.Process(func(ec ElmC, elm KV[K, Iter[taggedRawBytes]]) error {
		rawMap := make(map[int][][]byte)
		for raw := range elm.Value.All() {
			rawMap[raw.Tag] = append(rawMap[raw.Tag], raw.Payload)
		}
		fn.Output.Emit(ec, Pair(elm.Key, CoGBKResult{
			rawBuffers: rawMap,
		}))
		return nil
	})
}

func sliceIter[V Element](s []V) Iter[V] {
	cur := 0
	return Iter[V]{
		source: func() (V, bool) {
			if cur >= len(s) {
				var dummy V
				return dummy, false
			}
			v := s[cur]
			cur++
			return v, true
		},
	}
}
