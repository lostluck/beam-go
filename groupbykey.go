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
	"lostluck.dev/beam-go/internal/beamopts"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// Key is an [Element] that is also [comparable]. Distinct keys must have distinct
// binary encodings in order for runners to distinguish them.
// TODO, rename to Key.
type Keys interface {
	comparable
	Element
}

// GBK produces an output PCollection of values grouped by key.
func GBK[K Keys, V Element](s *Scope, input PCol[KV[K, V]], opts ...Options) PCol[KV[K, Iter[V]]] {
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}

	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.consumers[input.globalIndex] = append(s.g.consumers[input.globalIndex], edgeID)

	s.g.edges = append(s.g.edges, &edgeGBK[K, V]{index: edgeID, input: input.globalIndex, output: nodeID, opts: opt})
	s.g.nodes = append(s.g.nodes, &typedNode[KV[K, Iter[V]]]{index: nodeID, parentEdge: edgeID})

	return PCol[KV[K, Iter[V]]]{globalIndex: nodeID}
}

// EdgeGBK represents a Group By Key transform.
type edgeGBK[K Keys, V Element] struct {
	index edgeIndex

	input, output nodeIndex
	opts          beamopts.Struct
}

func (e *edgeGBK[K, V]) protoID() string {
	return "invalid-GBK-id"
}
func (e *edgeGBK[K, V]) edgeID() edgeIndex {
	return e.index
}

// inputs for GBKs are one.
func (e *edgeGBK[K, V]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

// outputs for GBKs are one.
func (e *edgeGBK[K, V]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeGBK[K, V]) toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	spec = &pipepb.FunctionSpec{Urn: "beam:transform:group_by_key:v1"}
	envID = "" // Runner transforms are left blank.
	name = "GroupByKey"
	return spec, envID, name
}

var _ protoDescMultiEdge = (*edgeGBK[int, int])(nil)

// Reshuffle inserts a fusion break in the pipeline, preventing a
// producer transform from being fused with the consuming transform.
func Reshuffle[E Element](s *Scope, input PCol[E], opts ...Options) PCol[E] {
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}

	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.consumers[input.globalIndex] = append(s.g.consumers[input.globalIndex], edgeID)

	s.g.edges = append(s.g.edges, &edgeReshuffle[E]{index: edgeID, input: input.globalIndex, output: nodeID, opts: opt})
	s.g.nodes = append(s.g.nodes, &typedNode[E]{index: nodeID, parentEdge: edgeID})

	return PCol[E]{globalIndex: nodeID}
}

// edgeReshuffle represents a
type edgeReshuffle[E Element] struct {
	index edgeIndex

	input, output nodeIndex
	opts          beamopts.Struct
}

func (e *edgeReshuffle[E]) protoID() string {
	return "invalid-Reshuffle-id"
}
func (e *edgeReshuffle[E]) edgeID() edgeIndex {
	return e.index
}

// inputs for Reshuffles are one.
func (e *edgeReshuffle[E]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

// outputs for Reshuffles are one.
func (e *edgeReshuffle[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeReshuffle[E]) toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	spec = &pipepb.FunctionSpec{Urn: "beam:transform:reshuffle:v1"}
	envID = "" // Runner transforms are left blank.
	name = "Reshuffle"
	return spec, envID, name
}
