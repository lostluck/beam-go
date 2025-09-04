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
	"reflect"

	"github.com/go-json-experiment/json"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/internal/beamopts"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// ParDo takes the users's DoFn and returns the same type for downstream piepline construction.
//
// The returned DoFn's emitter fields can then be used as inputs into other DoFns.
// What if we used Emitters as PCollections directly?
// Obviously, we'd rename the type PCollection or similar
// If only to also
func ParDo[E Element, DF Transform[E]](s *Scope, input PCol[E], dofn DF, opts ...Options) DF {
	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	ins, outs, sides, extras := s.g.deferDoFn(dofn, input.globalIndex, edgeID)

	s.g.edges = append(s.g.edges, &edgeDoFn[E]{index: edgeID, dofn: dofn, ins: ins, outs: outs, sides: sides, parallelIn: input.globalIndex, opts: opt, sdf: extras.SDF()})

	return dofn
}

func (g *graph) deferDoFn(dofn any, input nodeIndex, global edgeIndex) (ins, outs map[string]nodeIndex, sides map[string]string, extras *dofnExtras) {
	if g.consumers == nil {
		g.consumers = map[nodeIndex][]edgeIndex{}
	}
	g.consumers[input] = append(g.consumers[input], global)

	rv := reflect.ValueOf(dofn)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	ins = map[string]nodeIndex{
		"parallel": input,
	}
	sides = map[string]string{}
	outs = map[string]nodeIndex{}
	efaceRT := reflect.TypeOf((*emitIface)(nil)).Elem()
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		sf := rt.Field(i)
		if !fv.CanAddr() || !sf.IsExported() {
			continue
		}
		switch sf.Type.Kind() {
		case reflect.Array, reflect.Slice:
			// Should we also allow for maps? Holy shit, we could also allow for maps....
			ptrEt := reflect.PointerTo(sf.Type.Elem())
			if !ptrEt.Implements(efaceRT) {
				continue
			}
			// Slice or Array
			for j := 0; j < fv.Len(); j++ {
				fvj := fv.Index(j).Addr()
				g.initEmitter(fvj.Interface().(emitIface), global, input, fmt.Sprintf("%s%%%d", sf.Name, j), outs)
			}
		case reflect.Struct:
			fv = fv.Addr()
			switch feature := fv.Interface().(type) {
			case emitIface:
				g.initEmitter(feature, global, input, sf.Name, outs)
			case sideIface:
				sides[sf.Name] = feature.accessPatternUrn()
				g.initSideInput(feature, global, sf.Name, ins)
			case sdfHandler:
				if extras == nil {
					extras = &dofnExtras{}
				}
				if extras.sdf != nil {
					panic(fmt.Sprintf("DoFn %v may only have one SDF handler, but has %v and now %v at %v", rt, extras.sdf, feature, fv))
				}
				extras.sdf = feature

			case stateIface:
				if extras == nil {
					extras = &dofnExtras{}
				}
				if extras.states == nil {
					extras.states = map[string]stateIface{}
				}
				extras.states[sf.Name] = feature
			}
		case reflect.Chan:
			panic(fmt.Sprintf("field %v is a channel", fv))
		default:
			// Don't do anything with pointers, or other types.

		}
	}
	return ins, outs, sides, extras
}

type dofnExtras struct {
	sdf    sdfHandler
	states map[string]stateIface
}

func (x *dofnExtras) SDF() sdfHandler {
	if x == nil {
		return nil
	}
	return x.sdf
}

func (g *graph) initEmitter(emt emitIface, global edgeIndex, input nodeIndex, name string, outs map[string]nodeIndex) {
	localIndex := len(outs)
	globalIndex := g.curNodeIndex()
	emt.setPColKey(globalIndex, localIndex, nil)
	node := emt.newNode(globalIndex.String(), globalIndex, global, g.nodes[input].bounded())
	g.nodes = append(g.nodes, node)
	outs[name] = globalIndex
}

func (g *graph) initSideInput(si sideIface, global edgeIndex, name string, ins map[string]nodeIndex) {
	globalIndex := si.sideInput()
	// Put into a special side input consumers list?
	g.consumers[globalIndex] = append(g.consumers[globalIndex], global)
	ins[name] = globalIndex
}

type edgeDoFn[E Element] struct {
	index     edgeIndex
	transform string

	dofn       Transform[E]
	ins, outs  map[string]nodeIndex  // local field names to global collection ids.
	sides      map[string]string     // local id to side input access pattern URN
	states     map[string]stateIface // local id to state access pattern URN, this edgeDoFn must be wrapped with edgeKeyedDoFn.
	parallelIn nodeIndex
	sdf        sdfHandler

	opts beamopts.Struct
}

func (e *edgeDoFn[E]) protoID() string {
	return e.transform
}

func (e *edgeDoFn[E]) edgeID() edgeIndex {
	return e.index
}

func (e *edgeDoFn[E]) inputs() map[string]nodeIndex {
	return e.ins
}

func (e *edgeDoFn[E]) outputs() map[string]nodeIndex {
	return e.outs
}

func (e *edgeDoFn[E]) toProtoParts(params translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	dofn := e.actualTransform()
	rv := reflect.ValueOf(dofn)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	// Register types with the lookup table.
	rt := rv.Type()
	typeName := rt.PkgPath() + "." + rt.Name()
	params.TypeReg[typeName] = rt

	opts := e.options()
	if opts.Name == "" {
		name = typeName
	} else {
		name = opts.Name
	}

	wrap := dofnWrap{
		TypeName: typeName,
		DoFn:     dofn,
	}

	if e.sdf != nil {
		sdfRT := reflect.ValueOf(e.sdf).Type().Elem()
		wrap.SDFTypeName = sdfRT.Name()
		params.TypeReg[wrap.SDFTypeName] = sdfRT
	}

	wrappedPayload, err := json.Marshal(&wrap, json.DefaultOptionsV2(), jsonDoFnMarshallers())
	if err != nil {
		panic(err)
	}

	var sis map[string]*pipepb.SideInput
	if len(e.sides) > 0 {
		sis = map[string]*pipepb.SideInput{}
		for local, pattern := range e.sides {
			sis[local] = &pipepb.SideInput{
				AccessPattern: &pipepb.FunctionSpec{
					Urn: pattern,
				},
				ViewFn: &pipepb.FunctionSpec{
					Urn: "dummyViewFn",
				},
				WindowMappingFn: &pipepb.FunctionSpec{
					Urn: "dummyWindowMappingFn",
				},
			}
		}
	}

	var sts map[string]*pipepb.StateSpec
	if len(e.states) > 0 {
		sts = map[string]*pipepb.StateSpec{}
		for local, st := range e.states {
			sts[local] = st.toProtoParts(params)
		}
	}

	payloadProto := &pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn:     "beam:go:transform:dofn:v2",
			Payload: wrappedPayload,
		},
		SideInputs: sis,
		StateSpecs: sts,
	}

	if e.sdf != nil {
		payloadProto.RestrictionCoderId = e.addCoder(params.InternedCoders, params.Comps.GetCoders())
	}

	payload, _ := proto.Marshal(payloadProto)

	spec = &pipepb.FunctionSpec{
		Urn:     "beam:transform:pardo:v1",
		Payload: payload,
	}
	return spec, params.DefaultEnvID, name
}

type bundleProcer interface {
	protoDescMultiEdge

	// Make this a reflect.Type and avoid instance aliasing.
	// Then we can keep the graph around, for cheaper startup vs reparsing proto
	actualTransform() any
	options() beamopts.Struct
	transformID() string
}

func (e *edgeDoFn[E]) actualTransform() any {
	return e.dofn
}

func (e *edgeDoFn[E]) options() beamopts.Struct {
	return e.opts
}

func (e *edgeDoFn[E]) transformID() string {
	return e.transform
}

func (e *edgeDoFn[E]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	if e.sdf != nil {
		return e.sdf.addRestrictionCoder(intern, coders)
	}
	return ""
}

var _ bundleProcer = (*edgeDoFn[int])(nil)
