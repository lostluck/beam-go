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
	"strings"
	"time"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
	"lostluck.dev/beam-go/internal/harness"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/internal/pipelinex"
)

type dofnWrap struct {
	TypeName    string
	SDFTypeName string // for SDFs and other uses
	DoFn        any

	// The following fields must not be encoded, and instead
	// only be used for execution time feature passing.
	restrictionCoder string
}

func jsonDoFnMarshallers() json.Options {
	return json.WithMarshalers(
		json.JoinMarshalers(
			// Turn all beam mixins into {} by default, as state should be reconstrutable from
			// the types anyway.
			json.MarshalToFunc(func(enc *jsontext.Encoder, byp bypassInterface, opts json.Options) error {
				enc.WriteToken(jsontext.ObjectStart)
				enc.WriteToken(jsontext.ObjectEnd)
				return nil
			}),
			// No special handling for marshalling the DoFn otherwise.
		))
}

func jsonDoFnUnmarshallers(typeReg map[string]reflect.Type, name string) json.Options {
	return json.WithUnmarshalers(
		json.JoinUnmarshalers(
			// Handle mixins by skipping the values.
			json.UnmarshalFromFunc(func(dec *jsontext.Decoder, val bypassInterface, opts json.Options) error {
				return dec.SkipValue()
			}),
			json.UnmarshalFromFunc(func(dec *jsontext.Decoder, val *dofnWrap, opts json.Options) error {
				for {
					tok, err := dec.ReadToken()
					if err != nil {
						return err
					}
					switch tok.Kind() {
					case '"':
						switch tok.String() {
						case "TypeName":
							tok2, err := dec.ReadToken()
							if err != nil {
								return err
							}
							val.TypeName = tok2.String()
							continue
						case "SDFTypeName":
							tok2, err := dec.ReadToken()
							if err != nil {
								return err
							}
							val.SDFTypeName = tok2.String()
							continue
						case "DoFn":
							dofnRT, ok := typeReg[val.TypeName]
							if !ok {
								panic(fmt.Sprintf("unknown pardo in transform %v: payload %q", name, val.TypeName))
							}
							val.DoFn = reflect.New(dofnRT).Interface()
							if err := json.UnmarshalDecode(dec, val.DoFn, opts); err != nil {
								return err
							}
							_, err = dec.ReadToken() // '}' (finish reading the value)
							return err
						}
					}
				}
			})),
	)
}

type translateParams struct {
	DefaultEnvID   string
	TypeReg        map[string]reflect.Type
	InternedCoders map[string]string
	Comps          *pipepb.Components
}

// marshal turns a pipeline graph into a normalized Beam pipeline proto.
func (g *graph) marshal(typeReg map[string]reflect.Type) *pipepb.Pipeline {
	var roots []string

	defaultEnvID := "go"

	comps := &pipepb.Components{
		Transforms:   map[string]*pipepb.PTransform{},
		Pcollections: map[string]*pipepb.PCollection{},
		WindowingStrategies: map[string]*pipepb.WindowingStrategy{
			"global": {
				WindowFn: &pipepb.FunctionSpec{
					Urn: "beam:window_fn:global_windows:v1",
				},
				MergeStatus:   pipepb.MergeStatus_NON_MERGING,
				WindowCoderId: "gwc",
				Trigger: &pipepb.Trigger{
					Trigger: &pipepb.Trigger_Default_{Default: &pipepb.Trigger_Default{}},
				},
				AccumulationMode: pipepb.AccumulationMode_DISCARDING,
				OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
				ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
				AllowedLateness:  0,
				OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
			},
		},
		Coders: map[string]*pipepb.Coder{
			"gwc": {
				Spec: &pipepb.FunctionSpec{Urn: "beam:coder:global_window:v1"},
			},
		},
		Environments: map[string]*pipepb.Environment{
			defaultEnvID: {
				Urn:           "go",
				Payload:       nil,
				DisplayData:   nil,
				Capabilities:  nil,
				Dependencies:  nil,
				ResourceHints: nil,
			},
		},
	}
	internedCoders := map[string]string{}

	for i, edge := range g.edges {
		inputs := make(map[string]string)
		for name, in := range edge.inputs() {
			inputs[name] = in.String()
		}
		outputs := make(map[string]string)
		for name, out := range edge.outputs() {
			outputs[name] = out.String()
		}

		var spec *pipepb.FunctionSpec
		var uniqueName string
		envID := defaultEnvID
		switch e := edge.(type) {
		case protoDescMultiEdge:
			spec, envID, uniqueName = e.toProtoParts(translateParams{
				TypeReg:        typeReg,
				DefaultEnvID:   defaultEnvID,
				InternedCoders: internedCoders,
				Comps:          comps,
			})
		default:
			panic(fmt.Sprintf("unknown edge type %#v", e))
		}

		comps.Transforms[edgeIndex(i).String()] = &pipepb.PTransform{
			UniqueName:    uniqueName,
			Spec:          spec,
			Inputs:        inputs,
			Outputs:       outputs,
			EnvironmentId: envID,
			Annotations:   nil,
		}
	}

	bounded := func(n node) pipepb.IsBounded_Enum {
		if n.bounded() {
			return pipepb.IsBounded_BOUNDED
		}
		return pipepb.IsBounded_UNBOUNDED
	}

	for i, node := range g.nodes {
		comps.Pcollections[nodeIndex(i).String()] = &pipepb.PCollection{
			UniqueName:          nodeIndex(i).String(), //  TODO make this "Parent.Output"
			CoderId:             node.addCoder(internedCoders, comps.GetCoders()),
			IsBounded:           bounded(node),
			WindowingStrategyId: "global",
			DisplayData:         nil,
		}
	}

	pipe, err := pipelinex.Normalize(&pipepb.Pipeline{
		Components:       comps,
		RootTransformIds: roots,
		DisplayData:      nil,
		Requirements:     nil,
	})
	if err != nil {
		panic(err)
	}
	return pipe
}

func (n *typedNode[E]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	return addCoder[E](intern, coders)
}

// structuralCoder is a helper interface to handle structural types.
// Implementers must populate the coders map.
type structuralCoder interface {
	addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string
}

func putCoder(coders map[string]*pipepb.Coder, urn string, payload []byte, components []string) string {
	id := fmt.Sprintf("c%d", len(coders))
	coders[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     urn,
			Payload: payload,
		},
		ComponentCoderIds: components,
	}
	return id
}

func addCoder[E any](intern map[string]string, coders map[string]*pipepb.Coder) string {
	var t E
	at := any(t)
	rt := reflect.TypeOf(t)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if id, ok := intern[rt.PkgPath()+"."+rt.Name()]; ok {
		return id
	}

	var urn string
	if at, ok := at.(structuralCoder); ok {
		return at.addCoder(intern, coders)
	}

	switch rt.Kind() {
	case reflect.Slice:
		if rt.Elem().Kind() == reflect.Uint8 {
			urn = "beam:coder:bytes:v1"
		}
	case reflect.Bool:
		urn = "beam:coder:bool:v1"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		urn = "beam:coder:varint:v1"
	case reflect.Float32, reflect.Float64:
		urn = "beam:coder:double:v1"
	case reflect.String:
		urn = "beam:coder:string_utf8:v1"
	case reflect.Struct:
		return addRowCoder(rt, intern, coders)
	default:
		panic(fmt.Sprintf("unknown coder type: generic %T, resolved %v", t, rt))
	}

	return putCoder(coders, urn, nil, nil)

	// TODO
	// urnLengthPrefixCoder        = "beam:coder:length_prefix:v1"
	// urnStateBackedIterableCoder = "beam:coder:state_backed_iterable:v1"
	// urnWindowedValueCoder       = "beam:coder:windowed_value:v1"
	// urnParamWindowedValueCoder  = "beam:coder:param_windowed_value:v1"
	// urnTimerCoder               = "beam:coder:timer:v1"
	// urnNullableCoder            = "beam:coder:nullable:v1"
}

func (KV[K, V]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	kID, vID := addCoder[K](intern, coders), addCoder[V](intern, coders)
	return putCoder(coders, "beam:coder:kv:v1", nil, []string{kID, vID})
}

func (Iter[V]) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	vID := addCoder[V](intern, coders)
	return putCoder(coders, "beam:coder:iterable:v1", nil, []string{vID})
}

func addRowCoder(rt reflect.Type, intern map[string]string, coders map[string]*pipepb.Coder) string {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("type %v, error: %v", rt, e))
		}
	}()
	urn := "beam:coder:row:v1"

	schm := schemaType(rt)

	// Now we need to build the row coder.

	buf, err := proto.Marshal(schm)
	if err != nil {
		panic(err)
	}

	return putCoder(coders, urn, buf, nil)
}

var (
	rtTimeTime = reflect.TypeFor[time.Time]()
)

func schemaType(rt reflect.Type) *pipepb.Schema {
	schm := &pipepb.Schema{
		Id:     uuid.NewString(),
		Fields: []*pipepb.Field{},
	}

	// TODO Improve Special type Handling.
	switch rt {
	case rtTimeTime:
		schm.Fields = append(schm.Fields, &pipepb.Field{
			Id:   int32(0),
			Name: "time",
			Type: schemaFieldType(reflect.TypeFor[string]()),
		})
		return schm
	}

	noneExported := true
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		noneExported = false
		schm.Fields = append(schm.Fields, &pipepb.Field{
			Id:   int32(i),
			Name: sf.Name,
			Type: schemaFieldType(sf.Type),
		})
	}
	if noneExported && rt.NumField() == 0 {
		panic(fmt.Sprintf("%v doesn't export any fields", rt))
	}
	return schm
}

func schemaFieldType(ft reflect.Type) *pipepb.FieldType {
	sft := &pipepb.FieldType{}
	switch ft.Kind() {
	case reflect.Int, reflect.Int64:
		sft.TypeInfo = &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_INT64,
		}
	case reflect.Int32:
		sft.TypeInfo = &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_INT32,
		}
	case reflect.Float32:
		sft.TypeInfo = &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_FLOAT,
		}
	case reflect.Float64:
		sft.TypeInfo = &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_DOUBLE,
		}
	case reflect.String:
		sft.TypeInfo = &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_STRING,
		}
	case reflect.Struct:
		sft.TypeInfo = &pipepb.FieldType_RowType{
			RowType: &pipepb.RowType{
				Schema: schemaType(ft),
			},
		}
	default:
		panic("can't handle this field type yet: " + ft.String())
	}
	return sft
}

// edgePlaceholder represents a transform that can't be created at translation time.
// It needs to be produced while building the graph from a bundle descriptor, so
// the transform can use the real types from upstream or downstream transforms.
type edgePlaceholder struct {
	index     edgeIndex
	kind      string // Indicates what sort of node this is a placeholder for.
	transform string

	ins, outs map[string]nodeIndex
	payload   []byte
}

func (e *edgePlaceholder) protoID() string {
	return e.transform
}

func (e *edgePlaceholder) edgeID() edgeIndex {
	return e.index
}

func (e *edgePlaceholder) inputs() map[string]nodeIndex {
	return e.ins
}

func (e *edgePlaceholder) outputs() map[string]nodeIndex {
	return e.outs
}

func unmarshalToGraph(typeReg map[string]reflect.Type, pbd *fnpb.ProcessBundleDescriptor) *graph {
	var g graph
	g.stateUrl = pbd.GetStateApiServiceDescriptor().GetUrl()
	g.consumers = map[nodeIndex][]edgeIndex{}

	pcolParents := map[nodeIndex]edgeIndex{}
	pcolToIndex := map[string]nodeIndex{}
	for name := range pbd.GetPcollections() {
		// Get placeholder nodes in the graph, and avoid reconstructing nodes multiple times.
		// We can't create the final typedEdge here because we don't have the real element type,
		// just the coder. The coder doesn't fully specify type information.
		id := nodeIndex(len(g.nodes))
		g.nodes = append(g.nodes, nil)
		pcolToIndex[name] = id
	}

	routeInputs := func(pt *pipepb.PTransform, edgeID edgeIndex) map[string]nodeIndex {
		ret := map[string]nodeIndex{}
		for local, global := range pt.GetInputs() {
			id := pcolToIndex[global]
			ret[local] = id
			g.consumers[id] = append(g.consumers[id], edgeID)
		}
		return ret
	}
	routeOutputs := func(pt *pipepb.PTransform, parent edgeIndex) map[string]nodeIndex {
		ret := map[string]nodeIndex{}
		for local, global := range pt.GetOutputs() {
			id := pcolToIndex[global]
			ret[local] = id
			pcolParents[id] = parent
		}
		return ret
	}

	var placeholders []edgeIndex
	addPlaceholder := func(pt *pipepb.PTransform, name, kind string) {
		edgeID := g.curEdgeIndex()
		ins := routeInputs(pt, edgeID)
		outs := routeOutputs(pt, edgeID)
		// Add a dummy edge.
		g.edges = append(g.edges, &edgePlaceholder{
			index:     edgeID,
			transform: name,
			kind:      kind,
			ins:       ins, outs: outs,
			payload: pt.GetSpec().GetPayload(),
		})
		placeholders = append(placeholders, edgeID)
	}

	for tid, pt := range pbd.GetTransforms() {
		if len(pt.GetSubtransforms()) > 0 { // I don't think we need to worry about these though...
			panic(fmt.Sprintf("can't handle composites yet:, contained by %v", tid))
		}
		spec := pt.GetSpec()

		switch spec.GetUrn() {
		case "beam:transform:impulse:v1":
			for _, global := range pt.GetOutputs() {
				id := g.curEdgeIndex()
				g.edges = append(g.edges, &edgeImpulse{
					index:  id,
					output: pcolToIndex[global],
				})
			}
		case "beam:runner:source:v1":
			addPlaceholder(pt, tid, "source")
		case "beam:runner:sink:v1":
			addPlaceholder(pt, tid, "sink")
		case "beam:transform:flatten:v1":
			addPlaceholder(pt, tid, "flatten")
		case "beam:transform:group_by_key:v1":
			panic("Worker side GBKs unimplemented. Runner error.")
		case "beam:transform:pardo:v1",
			"beam:transform:combine_per_key_precombine:v1",
			"beam:transform:combine_per_key_merge_accumulators:v1",
			"beam:transform:combine_per_key_extract_outputs:v1",
			"beam:transform:sdf_pair_with_restriction:v1",
			"beam:transform:sdf_split_and_size_restrictions:v1",
			"beam:transform:sdf_process_sized_element_and_restrictions:v1":

			var wrap dofnWrap
			var userDoFn any
			switch spec.GetUrn() {
			case "beam:transform:pardo:v1":
				decodeDoFn(spec.GetPayload(), &wrap, typeReg, tid)
			case "beam:transform:sdf_pair_with_restriction:v1":
				decodeDoFn(spec.GetPayload(), &wrap, typeReg, tid)
				// Extract interesting transform here, like the SDF handler.
				sdfH := reflect.New(typeReg[wrap.SDFTypeName]).Interface().(sdfHandler)

				// Make it so the wrapper has a method to extract the user sdfHandler type.
				// Do the magic assertion so we can get the type correct implementation.
				wrap.DoFn = sdfH.pairWithRestriction()
			case "beam:transform:sdf_split_and_size_restrictions:v1":
				decodeDoFn(spec.GetPayload(), &wrap, typeReg, tid)
				sdfH := reflect.New(typeReg[wrap.SDFTypeName]).Interface().(sdfHandler)
				wrap.DoFn = sdfH.splitAndSizeRestriction()
			case "beam:transform:sdf_process_sized_element_and_restrictions:v1":
				decodeDoFn(spec.GetPayload(), &wrap, typeReg, tid)
				sdfH := reflect.New(typeReg[wrap.SDFTypeName]).Interface().(sdfHandler)
				userDoFn = wrap.DoFn
				// Get the input coder for split encoding.
				onlyInputID := maps.Values(pt.GetInputs())[0]
				coderID := pbd.GetPcollections()[onlyInputID].GetCoderId()
				wrap.DoFn = sdfH.processSizedElementAndRestriction(wrap.DoFn, pbd.GetCoders(), coderID, tid, coderID)
			case "beam:transform:combine_per_key_precombine:v1":
				decodeCombineFn(spec.GetPayload(), &wrap, typeReg, tid)
				cmb := wrap.DoFn.(combiner)
				wrap.DoFn = cmb.precombine()
			case "beam:transform:combine_per_key_merge_accumulators:v1":
				decodeCombineFn(spec.GetPayload(), &wrap, typeReg, tid)
				cmb := wrap.DoFn.(combiner)
				wrap.DoFn = cmb.mergeacuumulators()
			case "beam:transform:combine_per_key_extract_outputs:v1":
				decodeCombineFn(spec.GetPayload(), &wrap, typeReg, tid)
				cmb := wrap.DoFn.(combiner)
				wrap.DoFn = cmb.extactoutput()
			}
			dofnPtrRT := reflect.TypeOf(wrap.DoFn)
			pbm, ok := dofnPtrRT.MethodByName("ProcessBundle")
			if !ok {
				panic(fmt.Sprintf("type in transform %v doesn't have a ProcessBundle method: %v", tid, dofnPtrRT))
			}
			// Extract the DFC type, and produce an instance of it for our use.
			dfcRT := pbm.Type.In(1).Elem()
			dofnType := dofnPtrRT.Elem()
			proc := reflect.New(dfcRT).Interface().(processor)

			// Special handling for SDF Process component processSizedElementAndRestrictoon
			if userDoFn != nil {
				// We need the actual user DoFn for the correct outputs here, so we substitute at this point.
				dofnType = reflect.TypeOf(userDoFn).Elem()
			}

			edgeID := g.curEdgeIndex()
			ins := routeInputs(pt, edgeID)
			for _, global := range pt.GetInputs() {
				id := pcolToIndex[global]
				if g.nodes[id] == nil {
					pcol := pbd.GetPcollections()[global]
					tn := proc.produceTypedNode(global, id, pcol.GetIsBounded() == pipepb.IsBounded_BOUNDED)
					tn.initCoder(pcol.GetCoderId(), pbd.GetCoders())
					g.nodes[id] = tn
				}
			}
			outs := routeOutputs(pt, edgeID)
			for local, global := range pt.GetOutputs() {
				// Always extract against the DoFn, incase
				// we need to rename an output.
				// But we don't need to always override a node.
				emt, ok := getEmitIfaceByName(dofnType, local, outs)
				if !ok {
					panic(fmt.Sprintf("consistency error: transform %v of type %v has no output field named %v", tid, dofnType, local))
				}
				id := pcolToIndex[global]
				if g.nodes[id] == nil {
					pcol := pbd.GetPcollections()[global]
					tn := emt.newNode(global, id, edgeID, pcol.GetIsBounded() == pipepb.IsBounded_BOUNDED)
					tn.initCoder(pcol.GetCoderId(), pbd.GetCoders())
					g.nodes[id] = tn
				}
			}
			opt := beamopts.Struct{
				Name: tid,
			}

			g.edges = append(g.edges, proc.produceDoFnEdge(tid, edgeID, wrap.DoFn, ins, outs, opt))
		default:
			panic(fmt.Sprintf("translate failed: unknown urn: %q", spec.GetUrn()))
		}
	}

placeholderLoop:
	for _, edgeID := range placeholders {
		// Placeholders are almost exclusively "single type" nodes
		e := g.edges[edgeID].(*edgePlaceholder)
		// Check the inputs and outputs for actual node types.
		for _, nodeID := range e.inputs() {
			g.edges[edgeID] = g.nodes[nodeID].newTypeMultiEdge(e, pbd.GetCoders())
			continue placeholderLoop
		}
		for _, nodeID := range e.outputs() {
			g.edges[edgeID] = g.nodes[nodeID].newTypeMultiEdge(e, pbd.GetCoders())
			continue placeholderLoop
		}
		panic(fmt.Sprintf("couldn't create placeholder node: %+v", e))
	}
	return &g
}

// newTypeMultiEdge produces a child edge that can or produce this node as needed.
// This is required to be able to produce Source and Sink nodes of the right type
// at bundle processing at pipeline execution time, since we don't have a real type
// for them yet.
func (c *typedNode[E]) newTypeMultiEdge(ph *edgePlaceholder, cs map[string]*pipepb.Coder) multiEdge {
	switch ph.kind {
	case "flatten":
		out := getSingleValue(ph.outs)
		return &edgeFlatten[E]{index: ph.index, transform: ph.transform, ins: maps.Values(ph.ins), output: out}
	case "source":
		port, cid, err := decodePort(ph.payload)
		if err != nil {
			panic(err)
		}
		out := getSingleValue(ph.outs)
		// TODO, extract windowed value coder for header
		return &edgeDataSource[E]{index: ph.index, transform: ph.transform, output: out, port: port,
			makeCoder: func() coders.Coder[E] { return coderFromProto[E](cs, cid) }}
	case "sink":
		port, cid, err := decodePort(ph.payload)
		if err != nil {
			panic(err)
		}
		in := getSingleValue(ph.ins)
		// TODO, extract windowed value coder for header
		return &edgeDataSink[E]{index: ph.index, transform: ph.transform, input: in, port: port,
			makeCoder: func() coders.Coder[E] { return coderFromProto[E](cs, cid) }}
	default:
		panic(fmt.Sprintf("unknown placeholder kind: %v", ph.kind))
	}
}

func decodePort(data []byte) (harness.Port, string, error) {
	var port fnpb.RemoteGrpcPort
	if err := proto.Unmarshal(data, &port); err != nil {
		return harness.Port{}, "", err
	}
	return harness.Port{
		URL: port.GetApiServiceDescriptor().GetUrl(),
	}, port.CoderId, nil
}

var (
	efaceRT               = reflect.TypeOf((*emitIface)(nil)).Elem()
	procSizedElmAndRestRT = reflect.TypeOf((*procSizedElmAndRestIface)(nil)).Elem()
)

// getEmitIfaceByName extracts an emitter from the DoFn so we can get the exact type
// for downstream node construction.
//
// TODO, consider detecting slice/array emitters earlier so we can
// avoid constantly reparsing the field name.
func getEmitIfaceByName(doFnT reflect.Type, field string, outs map[string]nodeIndex) (emitIface, bool) {
	splitName := strings.Split(field, "%")

	switch len(splitName) {
	case 1:
		sf, ok := doFnT.FieldByName(field)
		if !ok {
			break
		}
		return reflect.New(sf.Type).Interface().(emitIface), true
	case 2:
		sf, ok := doFnT.FieldByName(splitName[0])
		if !ok {
			break
		}
		return reflect.New(sf.Type.Elem()).Interface().(emitIface), true
	}
	// OK, we have a manufactured name from the runner.
	// That means this is probably a built-in transform, so it may not
	// match any SDK side output name.
	// Everything is fine iff there's only one emitter, so lets iterate and check all the fields.
	// We only need the actual type anyway.
	var rt reflect.Type
	var rename string
	for i := 0; i < doFnT.NumField(); i++ {
		sf := doFnT.Field(i)
		if reflect.PointerTo(sf.Type).Implements(efaceRT) {
			if rt != nil {
				return nil, false
			}
			rt = sf.Type
			rename = sf.Name
		}
	}
	if rt == nil {
		panic(doFnT.Name())
	}
	// Ensure downstream uses of the fieldname are updated.
	outs[rename] = outs[field]
	delete(outs, field)
	return reflect.New(rt).Interface().(emitIface), true
}

func decodeDoFn(payload []byte, wrap *dofnWrap, typeReg map[string]reflect.Type, name string) {
	var dofnPayload pipepb.ParDoPayload
	if err := proto.Unmarshal(payload, &dofnPayload); err != nil {
		panic(err)
	}
	dofnSpec := dofnPayload.GetDoFn()

	if dofnSpec.GetUrn() != "beam:go:transform:dofn:v2" {
		panic(fmt.Sprintf("unknown pardo urn in transform %q: urn %q\n", name, dofnSpec.GetUrn()))
	}

	if dofnPayload.GetRequestsFinalization() {
		panic(fmt.Sprintf("Bundle Finalization isn't yet supported, required by DoFn: %q", name))
	}

	if len(dofnPayload.GetStateSpecs())+len(dofnPayload.GetTimerFamilySpecs()) > 0 {
		panic(fmt.Sprintf("State and Timers aren't yet supported, required by DoFn: %q", name))
	}

	if dofnPayload.GetOnWindowExpirationTimerFamilySpec() != "" {
		panic(fmt.Sprintf("OnWindowExpiration isn't yet supported, required by DoFn: %q", name))
	}

	if dofnPayload.GetRestrictionCoderId() != "" {
		wrap.restrictionCoder = dofnPayload.GetRestrictionCoderId()
	}

	if err := json.Unmarshal(dofnSpec.GetPayload(), &wrap, json.DefaultOptionsV2(), jsonDoFnUnmarshallers(typeReg, name)); err != nil {
		panic(err)
	}
}

func decodeCombineFn(payload []byte, wrap *dofnWrap, typeReg map[string]reflect.Type, name string) {
	var combineFnPayload pipepb.CombinePayload
	if err := proto.Unmarshal(payload, &combineFnPayload); err != nil {
		panic(err)
	}
	combineFnSpec := combineFnPayload.GetCombineFn()

	if combineFnSpec.GetUrn() != "beam:go:transform:dofn:v2" {
		panic(fmt.Sprintf("unknown pardo urn in transform %q: urn %q\n", name, combineFnSpec.GetUrn()))
	}

	if err := json.Unmarshal(combineFnSpec.GetPayload(), &wrap, json.DefaultOptionsV2(), jsonDoFnUnmarshallers(typeReg, name)); err != nil {
		panic(err)
	}
}
