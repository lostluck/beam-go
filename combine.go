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
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// Design Goals:
// Only have a CombinePerKey and GlobalCombine top level methods for attaching to the graph.
// They will both use the same Combiner type as the parameter.

// AccumulatorMerger is an interface for combiners that only need a binary merge,
// and the input, output, and accumulator types are all the same.
type AccumulatorMerger[A Element] interface {
	MergeAccumulators(A, A) A
}

// AccumulatorCreator is an interface to allow combiners to produce a more
// sophisticated accumulator type, when the zero value is inappropriate for
// accumulation.
type AccumulatorCreator[A Element] interface {
	CreateAccumulator() A
	AccumulatorMerger[A]
}

// InputAdder is an interface to allow combiners to incorporate an input type
type InputAdder[A, I Element] interface {
	AddInput(A, I) A
	AccumulatorMerger[A]
}

type OutputExtractor[A, O Element] interface {
	AccumulatorMerger[A]
	ExtractOutput(A) O
}

type FullCombiner[A, I, O Element] interface {
	InputAdder[A, I]
	AccumulatorMerger[A]
	OutputExtractor[A, O]
}

// Combiners represent an optimizable approach to aggregating, by breaking down
// the aggregation into 3 component types.
type Combiner[A, I, O Element, AM AccumulatorMerger[A]] struct {
	// By having the AccumulatorMerger as part of the Combiner type, we get
	// simpler registration/serialization of
	am AM
}

// SimpleMerge produces a Combiner from an AccumulatorMerger.
func SimpleMerge[A Element, AM AccumulatorMerger[A]](c AM) Combiner[A, A, A, AM] {
	return Combiner[A, A, A, AM]{am: c}
}

// AddMerge produces a Combiner from an InputAdder.
func AddMerge[A, I Element, IA InputAdder[A, I]](c IA) Combiner[A, I, A, IA] {
	return Combiner[A, I, A, IA]{am: c}
}

// MergeExtract produces a Combiner from an OutputExtractor.
func MergeExtract[A, O Element, OE OutputExtractor[A, O]](c OE) Combiner[A, A, O, OE] {
	return Combiner[A, A, O, OE]{am: c}
}

// MergeExtract produces a Combiner from a FullCombiner.
func FullCombine[A, I, O Element, C FullCombiner[A, I, O]](c C) Combiner[A, I, O, C] {
	return Combiner[A, I, O, C]{am: c}
}

// We can't simply make these methods on Combiner because PerKey needs an additional
// type for the key. It would be awkward to just have Globally as a method.

func CombinePerKey[K Keys, A, I, O Element, AM AccumulatorMerger[A]](s *Scope, input PCol[KV[K, I]], comb Combiner[A, I, O, AM]) PCol[KV[K, O]] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.edges = append(s.g.edges, &edgeCombine{index: edgeID, input: input.globalIndex, output: nodeID, comb: &hiddenKeyedCombiner[K, A, I, O, AM]{Merger: comb.am}})
	s.g.nodes = append(s.g.nodes, &typedNode[KV[K, O]]{index: nodeID, parentEdge: edgeID})
	return PCol[KV[K, O]]{globalIndex: nodeID}
}

// edgeCombine represents a combine transform.
type edgeCombine struct {
	index edgeIndex
	comb  combiner

	input, output nodeIndex
}

func (e *edgeCombine) protoID() string {
	return "invalid-combine-id"
}

func (e *edgeCombine) edgeID() edgeIndex {
	return e.index
}

// inputs for combines are one.
func (e *edgeCombine) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"parallel": e.input}
}

// outputs for combines are one.
func (e *edgeCombine) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"Output": e.output}
}

func (e *edgeCombine) toProtoParts(params translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	cfn := e.comb
	rv := reflect.ValueOf(cfn)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	// Register types with the lookup table.
	typeName := rv.Type().Name()
	params.TypeReg[typeName] = rv.Type()

	name = typeName

	wrap := dofnWrap{
		TypeName: typeName,
		DoFn:     cfn,
	}
	wrappedPayload, err := json.Marshal(&wrap, json.DefaultOptionsV2(), jsonDoFnMarshallers())
	if err != nil {
		panic(err)
	}

	payload, _ := proto.Marshal(&pipepb.CombinePayload{
		CombineFn: &pipepb.FunctionSpec{
			Urn:     "beam:go:transform:dofn:v2",
			Payload: wrappedPayload,
		},
		AccumulatorCoderId: e.addCoder(params.InternedCoders, params.Comps.GetCoders()),
	})

	spec = &pipepb.FunctionSpec{
		Urn:     "beam:transform:combine_per_key:v1",
		Payload: payload,
	}
	return spec, params.DefaultEnvID, name
}

func (n *edgeCombine) addCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	return n.comb.addAccumCoder(intern, coders)
}

// liftedCombine represents a pre-GBK combining stage.
// The goal is typically to reduce the amount data being sent to a GBK stage.
//
// TODO: Would it be better to have separate executions for when AddInput exists or not,
// picked at graph build time?
// Probably is, because then we have the correct type for the DFC.
type liftedAddingCombine[K Keys, I, A Element] struct {
	KeyCoder coders.Coder[K]

	Merger AccumulatorMerger[A]

	// TODO implement and use WindowObserver
	Output PCol[KV[K, A]]
	OnBundleFinish
	ObserveWindow
}

func (fn *liftedAddingCombine[K, I, A]) ProcessBundle(dfc *DFC[KV[K, I]]) error {
	// TODO, add KeyObserver so combines can access the key if needed.
	// TODO, add a MetricsObserver
	// TODO, add a context observer to get a "real" context from this.
	// Perhaps these are all a single "Observer" type.
	createA := func() A {
		var a A
		return a
	}
	if ca, ok := fn.Merger.(AccumulatorCreator[A]); ok {
		createA = ca.CreateAccumulator
	}

	// Currently cheating, use the KeyCoder, and efficient byte to string conversions for lookup
	// TODO also have a layer for windows.
	// TODO allow for ElmC caching for picking merge timestamps.
	cache := map[K]A{}

	const cacheMax = 10000

	ai, ok := fn.Merger.(InputAdder[A, I])
	if !ok {
		panic(fmt.Errorf("combiner %T doesn't support the AddInput method type", fn.Merger))
	}
	var prevElmC ElmC
	dfc.Process(func(ec ElmC, elm KV[K, I]) error {
		prevElmC = ec
		a, ok := cache[elm.Key]
		if !ok {
			a = createA()
		}
		a = ai.AddInput(a, elm.Value)
		cache[elm.Key] = a

		for k, ca := range cache {
			// If the cache is small enough, no evictions.
			if len(cache) < cacheMax {
				return nil
			}
			if k == elm.Key {
				continue // never evict the current key. Leads to post grouping errors.
			}
			delete(cache, k) // Remove this key and accumulator.

			// TODO, use a proper timestamp & window to make the ElmC.
			fn.Output.Emit(ec, KV[K, A]{Key: k, Value: ca})
		}
		return nil
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		for k, ca := range cache {
			fn.Output.Emit(prevElmC, KV[K, A]{Key: k, Value: ca})
		}
		return nil
	})
	return nil
}

type liftedMergedCombine[K Keys, A Element] struct {
	KeyCoder coders.Coder[K]

	Merger AccumulatorMerger[A]

	// TODO implement and use WindowObserver
	Output PCol[KV[K, A]]
	OnBundleFinish
	ObserveWindow
}

func (fn *liftedMergedCombine[K, A]) ProcessBundle(dfc *DFC[KV[K, A]]) error {
	// TODO, add KeyObserver so combines can access the key if needed.
	// TODO, add a MetricsObserver
	// TODO, add a context observer to get a "real" context from this.
	// Perhaps these are all a single "Observer" type.
	createA := func() A {
		var a A
		return a
	}
	if ca, ok := fn.Merger.(AccumulatorCreator[A]); ok {
		createA = ca.CreateAccumulator
	}

	// Currently cheating, use the KeyCoder, and efficient byte to string conversions for lookup
	// TODO also have a layer for windows.
	// TODO allow for ElmC caching for picking merge timestamps.
	cache := map[K]A{}

	const cacheMax = 10000

	var prevElmC ElmC
	dfc.Process(func(ec ElmC, elm KV[K, A]) error {
		prevElmC = ec
		a, ok := cache[elm.Key]
		if !ok {
			a = createA()
		}
		a = fn.Merger.MergeAccumulators(a, elm.Value)
		cache[elm.Key] = a

		for k, ca := range cache {
			// If the cache is small enough, no evictions.
			if len(cache) < cacheMax {
				return nil
			}
			if k == elm.Key {
				continue // never evict the current key. Leads to post grouping errors.
			}
			delete(cache, k) // Remove this key and accumulator.

			// TODO, use a proper timestamp & window to make the ElmC.
			fn.Output.Emit(ec, KV[K, A]{Key: k, Value: ca})
		}
		return nil
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		for k, ca := range cache {
			fn.Output.Emit(prevElmC, KV[K, A]{Key: k, Value: ca})
		}
		return nil
	})
	return nil
}

type mergingKeyedCombine[K Keys, A Element] struct {
	Merger AccumulatorMerger[A]

	Output PCol[KV[K, A]]
}

func (fn *mergingKeyedCombine[K, A]) ProcessBundle(dfc *DFC[KV[K, Iter[A]]]) error {
	createA := func() A {
		var a A
		return a
	}
	if ca, ok := fn.Merger.(AccumulatorCreator[A]); ok {
		createA = ca.CreateAccumulator
	}
	dfc.Process(func(ec ElmC, elm KV[K, Iter[A]]) error {
		a := createA()
		elm.Value.All()(func(elm A) bool {
			a = fn.Merger.MergeAccumulators(a, elm)
			return true
		})
		fn.Output.Emit(ec, KV[K, A]{Key: elm.Key, Value: a})
		return nil
	})
	return nil
}

type outputExtractingKeyedCombine[K Keys, A, O Element] struct {
	KeyCoder coders.Coder[K]

	Merger AccumulatorMerger[A]

	// TODO implement and use WindowObserver
	Output PCol[KV[K, O]]
	OnBundleFinish
}

func (fn *outputExtractingKeyedCombine[K, A, O]) ProcessBundle(dfc *DFC[KV[K, A]]) error {
	oe, ok := fn.Merger.(OutputExtractor[A, O])
	if !ok {
		return fmt.Errorf("combiner %T doesn't support the AddInput method type", fn.Merger)
	}
	dfc.Process(func(ec ElmC, elm KV[K, A]) error {
		fn.Output.Emit(ec, KV[K, O]{Key: elm.Key, Value: oe.ExtractOutput(elm.Value)})
		return nil
	})
	return nil
}

type identityFn[E Element] struct {
	Output PCol[E]
}

func (fn *identityFn[E]) ProcessBundle(dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		fn.Output.Emit(ec, elm)
		return nil
	})
	return nil
}

type hiddenKeyedCombiner[K Keys, A, I, O Element, AM AccumulatorMerger[A]] struct {
	Merger AM
}

func (*hiddenKeyedCombiner[K, A, I, O, AM]) addAccumCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	return addCoder[A](intern, coders)
}

func (c *hiddenKeyedCombiner[K, A, I, O, AM]) precombine() any {
	a := any(c.Merger)
	if _, ok := a.(InputAdder[A, I]); ok {
		return &liftedAddingCombine[K, I, A]{
			Merger: c.Merger,
		}
	}
	return &liftedMergedCombine[K, A]{
		Merger: c.Merger,
	}
}

func (c *hiddenKeyedCombiner[K, A, I, O, AM]) mergeacuumulators() any {
	return &mergingKeyedCombine[K, A]{
		Merger: c.Merger,
	}
}

func (c *hiddenKeyedCombiner[K, A, I, O, AM]) extactoutput() any {
	a := any(c.Merger)
	if _, ok := a.(OutputExtractor[A, O]); ok {
		return &outputExtractingKeyedCombine[K, A, O]{
			Merger: c.Merger,
		}
	}
	return &identityFn[KV[K, A]]{}
}

type combiner interface {
	addAccumCoder(intern map[string]string, coders map[string]*pipepb.Coder) string
	precombine() any
	mergeacuumulators() any
	extactoutput() any
}

var _ combiner = &hiddenKeyedCombiner[int, int, int, int, AccumulatorMerger[int]]{}
