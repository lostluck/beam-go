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
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// Impulse adds an impulse transform to the graph, which emits single element
// to downstream transforms, allowing processing to begin.
//
// The element is a single byte slice in the global window, with an event timestamp
// at the start of the global window.
func Impulse(s *Scope) PCol[[]byte] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.edges = append(s.g.edges, &edgeImpulse{index: edgeID, output: nodeID})
	s.g.nodes = append(s.g.nodes, &typedNode[[]byte]{index: nodeID, parentEdge: edgeID})
	return PCol[[]byte]{globalIndex: nodeID}
}

// edgeImpulse represents an Impulse transform.
type edgeImpulse struct {
	index  edgeIndex
	output nodeIndex
}

func (e *edgeImpulse) protoID() string {
	return "invalid-impulse-id"
}

func (e *edgeImpulse) edgeID() edgeIndex {
	return e.index
}

// inputs for impulses are nil.
func (e *edgeImpulse) inputs() map[string]nodeIndex {
	return nil
}

// inputs for impulses are one.
func (e *edgeImpulse) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeImpulse) toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	spec = &pipepb.FunctionSpec{Urn: "beam:transform:impulse:v1"}
	envID = "" // Runner transforms are left blank.
	name = "Impulse"
	return spec, envID, name
}

var _ protoDescMultiEdge = (*edgeImpulse)(nil)
