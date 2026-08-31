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

// Package joins provides convenience relational join transforms built on top of CoGBK.
package joins

import (
	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/internal/beamopts"
)

// InnerJoin returns a PCollection of pairs for keys present in both input collections.
// For keys present in both collections, produces the Cartesian product of values.
func InnerJoin[K beam.Keys, V1, V2 beam.Element](
	s *beam.Scope,
	col1 beam.PCol[beam.KV[K, V1]],
	col2 beam.PCol[beam.KV[K, V2]],
	opts ...beamopts.Options,
) beam.PCol[beam.KV[K, beam.KV[V1, V2]]] {
	grouped := s.CoGBK2(col1, col2, opts...)
	dofn := s.ParDo(grouped, &innerJoinFn[K, V1, V2]{})
	return dofn.Output
}

type innerJoinFn[K beam.Keys, V1, V2 beam.Element] struct {
	Output beam.PCol[beam.KV[K, beam.KV[V1, V2]]]
}

func (fn *innerJoinFn[K, V1, V2]) ProcessBundle(dfc *beam.DFC[beam.KV[K, beam.CoGBKResult2[V1, V2]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[K, beam.CoGBKResult2[V1, V2]]) error {
		var s1 []V1
		for v1 := range elm.Value.Val1.All() {
			s1 = append(s1, v1)
		}
		if len(s1) == 0 {
			return nil
		}
		var s2 []V2
		for v2 := range elm.Value.Val2.All() {
			s2 = append(s2, v2)
		}
		if len(s2) == 0 {
			return nil
		}
		for _, v1 := range s1 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, v2)))
			}
		}
		return nil
	})
}

// LeftJoin returns pairs for all keys present in col1. If a key has no values in col2,
// defaultV2 is paired with the col1 value.
func LeftJoin[K beam.Keys, V1, V2 beam.Element](
	s *beam.Scope,
	col1 beam.PCol[beam.KV[K, V1]],
	col2 beam.PCol[beam.KV[K, V2]],
	defaultV2 V2,
	opts ...beamopts.Options,
) beam.PCol[beam.KV[K, beam.KV[V1, V2]]] {
	grouped := s.CoGBK2(col1, col2, opts...)
	dofn := s.ParDo(grouped, &leftJoinFn[K, V1, V2]{DefaultV2: defaultV2})
	return dofn.Output
}

type leftJoinFn[K beam.Keys, V1, V2 beam.Element] struct {
	DefaultV2 V2
	Output    beam.PCol[beam.KV[K, beam.KV[V1, V2]]]
}

func (fn *leftJoinFn[K, V1, V2]) ProcessBundle(dfc *beam.DFC[beam.KV[K, beam.CoGBKResult2[V1, V2]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[K, beam.CoGBKResult2[V1, V2]]) error {
		var s1 []V1
		for v1 := range elm.Value.Val1.All() {
			s1 = append(s1, v1)
		}
		if len(s1) == 0 {
			return nil
		}
		var s2 []V2
		for v2 := range elm.Value.Val2.All() {
			s2 = append(s2, v2)
		}
		if len(s2) == 0 {
			for _, v1 := range s1 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, fn.DefaultV2)))
			}
			return nil
		}
		for _, v1 := range s1 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, v2)))
			}
		}
		return nil
	})
}

// RightJoin returns pairs for all keys present in col2. If a key has no values in col1,
// defaultV1 is paired with the col2 value.
func RightJoin[K beam.Keys, V1, V2 beam.Element](
	s *beam.Scope,
	col1 beam.PCol[beam.KV[K, V1]],
	col2 beam.PCol[beam.KV[K, V2]],
	defaultV1 V1,
	opts ...beamopts.Options,
) beam.PCol[beam.KV[K, beam.KV[V1, V2]]] {
	grouped := s.CoGBK2(col1, col2, opts...)
	dofn := s.ParDo(grouped, &rightJoinFn[K, V1, V2]{DefaultV1: defaultV1})
	return dofn.Output
}

type rightJoinFn[K beam.Keys, V1, V2 beam.Element] struct {
	DefaultV1 V1
	Output    beam.PCol[beam.KV[K, beam.KV[V1, V2]]]
}

func (fn *rightJoinFn[K, V1, V2]) ProcessBundle(dfc *beam.DFC[beam.KV[K, beam.CoGBKResult2[V1, V2]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[K, beam.CoGBKResult2[V1, V2]]) error {
		var s2 []V2
		for v2 := range elm.Value.Val2.All() {
			s2 = append(s2, v2)
		}
		if len(s2) == 0 {
			return nil
		}
		var s1 []V1
		for v1 := range elm.Value.Val1.All() {
			s1 = append(s1, v1)
		}
		if len(s1) == 0 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(fn.DefaultV1, v2)))
			}
			return nil
		}
		for _, v1 := range s1 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, v2)))
			}
		}
		return nil
	})
}

// FullOuterJoin returns pairs for all keys present in either col1 or col2.
// Missing sides are filled with defaultV1 or defaultV2 respectively.
func FullOuterJoin[K beam.Keys, V1, V2 beam.Element](
	s *beam.Scope,
	col1 beam.PCol[beam.KV[K, V1]],
	col2 beam.PCol[beam.KV[K, V2]],
	defaultV1 V1,
	defaultV2 V2,
	opts ...beamopts.Options,
) beam.PCol[beam.KV[K, beam.KV[V1, V2]]] {
	grouped := s.CoGBK2(col1, col2, opts...)
	dofn := s.ParDo(grouped, &fullOuterJoinFn[K, V1, V2]{DefaultV1: defaultV1, DefaultV2: defaultV2})
	return dofn.Output
}

type fullOuterJoinFn[K beam.Keys, V1, V2 beam.Element] struct {
	DefaultV1 V1
	DefaultV2 V2
	Output    beam.PCol[beam.KV[K, beam.KV[V1, V2]]]
}

func (fn *fullOuterJoinFn[K, V1, V2]) ProcessBundle(dfc *beam.DFC[beam.KV[K, beam.CoGBKResult2[V1, V2]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[K, beam.CoGBKResult2[V1, V2]]) error {
		var s1 []V1
		for v1 := range elm.Value.Val1.All() {
			s1 = append(s1, v1)
		}
		var s2 []V2
		for v2 := range elm.Value.Val2.All() {
			s2 = append(s2, v2)
		}
		if len(s1) == 0 && len(s2) == 0 {
			return nil
		}
		if len(s1) == 0 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(fn.DefaultV1, v2)))
			}
			return nil
		}
		if len(s2) == 0 {
			for _, v1 := range s1 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, fn.DefaultV2)))
			}
			return nil
		}
		for _, v1 := range s1 {
			for _, v2 := range s2 {
				fn.Output.Emit(ec, beam.Pair(elm.Key, beam.Pair(v1, v2)))
			}
		}
		return nil
	})
}
