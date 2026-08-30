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
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

type edgeTestStream[E Element] struct {
	index     edgeIndex
	output    nodeIndex
	payloadFn func(coder coders.Coder[E], coderID string) (*pipepb.TestStreamPayload, error)
}

func (e *edgeTestStream[E]) protoID() string {
	return "TestStream"
}

func (e *edgeTestStream[E]) edgeID() edgeIndex {
	return e.index
}

func (e *edgeTestStream[E]) inputs() map[string]nodeIndex {
	return nil
}

func (e *edgeTestStream[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeTestStream[E]) toProtoParts(params translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	coderID := params.Graph.nodes[e.output].addCoder(params.InternedCoders, params.Comps.Coders)
	coder := coderFromProto[E](params.Comps.Coders, coderID)

	payloadPb, err := e.payloadFn(coder, coderID)
	if err != nil {
		panic(err)
	}

	payload, err := proto.Marshal(payloadPb)
	if err != nil {
		panic(err)
	}

	spec = &pipepb.FunctionSpec{
		Urn:     "beam:transform:test_stream:v1",
		Payload: payload,
	}
	envID = "" // Runner primitive transform
	name = "TestStream"
	return spec, envID, name
}

// TestStream attaches a TestStream runner primitive transform to the Scope graph,
// returning an unbounded PCol[E].
//
// Users should generally prefer using the lostluck.dev/beam-go/transforms/testing/teststream
// package for a fluent builder API.
func (s *Scope) TestStream[E Element](payloadFn func(coder coders.Coder[E], coderID string) (*pipepb.TestStreamPayload, error)) PCol[E] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.edges = append(s.g.edges, &edgeTestStream[E]{index: edgeID, output: nodeID, payloadFn: payloadFn})
	s.g.nodes = append(s.g.nodes, &typedNode[E]{index: nodeID, parentEdge: edgeID, isBounded: false})
	return PCol[E]{valid: true, globalIndex: nodeID}
}
