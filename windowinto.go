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
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
)

// WindowInto applies a windowing strategy to the input PCollection.
func (s *Scope) WindowInto[E Element](input PCol[E], winFn window.WindowFn, opts ...window.WindowOption) PCol[E] {
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}

	strat := window.NewStrategy(winFn, opts...)

	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.consumers[input.globalIndex] = append(s.g.consumers[input.globalIndex], edgeID)

	s.g.edges = append(s.g.edges, &edgeWindowInto[E]{
		index:    edgeID,
		input:    input.globalIndex,
		output:   nodeID,
		strategy: strat,
	})
	s.g.nodes = append(s.g.nodes, &typedNode[E]{
		index:          nodeID,
		parentEdge:     edgeID,
		isBounded:      s.g.nodes[input.globalIndex].bounded(),
		windowStrategy: strat,
	})

	return PCol[E]{valid: true, globalIndex: nodeID}
}

type edgeWindowInto[E Element] struct {
	index     edgeIndex
	transform string
	input     nodeIndex
	output    nodeIndex
	strategy  *window.Strategy

	instance *windowIntoDoFn[E]
	procs    []processor
}

func (e *edgeWindowInto[E]) protoID() string {
	if e.transform != "" {
		return e.transform
	}
	return "WindowInto"
}

func (e *edgeWindowInto[E]) edgeID() edgeIndex {
	return e.index
}

func (e *edgeWindowInto[E]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

func (e *edgeWindowInto[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeWindowInto[E]) toProtoParts(params translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	var payload []byte
	if e.strategy != nil && e.strategy.Fn != nil {
		payload, _ = proto.Marshal(&pipepb.WindowIntoPayload{
			WindowFn: e.strategy.Fn.ToProto(),
		})
	}
	spec = &pipepb.FunctionSpec{
		Urn:     "beam:transform:window_into:v1",
		Payload: payload,
	}
	envID = params.DefaultEnvID
	name = "WindowInto"
	return spec, envID, name
}

func (e *edgeWindowInto[E]) windowInto() (string, any, []processor) {
	if e.instance == nil {
		e.instance = &windowIntoDoFn[E]{
			Output:   PCol[E]{valid: true, globalIndex: e.output, localDownstreamIndex: 0},
			Strategy: e.strategy,
		}
		e.procs = []processor{e.instance.Output.newDFC(e.output)}
	}
	return "WindowInto", e.instance, e.procs
}

type windowIntor interface {
	protoDescMultiEdge
	windowInto() (string, any, []processor)
}

var _ windowIntor = (*edgeWindowInto[int])(nil)

type windowIntoDoFn[E Element] struct {
	Output   PCol[E]
	Strategy *window.Strategy
}

func (fn *windowIntoDoFn[E]) ProcessBundle(dfc *DFC[E]) error {
	return dfc.Process(func(ec ElmC, elm E) error {
		if fn.Strategy != nil && fn.Strategy.Fn != nil {
			windows := fn.Strategy.Fn.AssignWindows(ec.EventTime())
			subEC := ElmC{
				eventTime:    ec.EventTime(),
				windows:      windows,
				window:       nil,
				pane:         ec.pane,
				pcollections: ec.pcollections,
			}
			fn.Output.Emit(subEC, elm)
			return nil
		}
		fn.Output.Emit(ec, elm)
		return nil
	})
}
