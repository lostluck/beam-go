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
	"math/rand/v2"
	"sync"

	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// dofns.go is about the different mix-ins and addons that can be added.

// beamMixin is added to all DoFn beam field types to allow them to bypass
// encoding. Only needed when the value has state and shouldn't be embedded.
type beamMixin struct{}

func (beamMixin) beamBypass() {}

type bypassInterface interface {
	beamBypass()
}

// PCol or PCollection represents an a logical collection of elements produced,
// or consumed by of a DoFn.
//
// At pipeline execution time, they are used in a ProcessBundle method to emit
// elements and pass along per element context, such as the EventTime and Window.
//
// Used as an Exported value field of a DoFn struct, they represent the outputs
// from the DoFn. After the DoFn is added to the graph, the processed DoFn's
// PCol fields are initialized and can be passed around by value, to further
// build the pipeline graph.
type PCol[E Element] struct {
	beamMixin

	valid                bool
	globalIndex          nodeIndex
	localDownstreamIndex int
	mets                 *pcollectionMetrics
	coder                coders.Coder[E]
}

type emitIface interface {
	setPColKey(global nodeIndex, id int, coder any) *pcollectionMetrics
	newDFC(id nodeIndex) processor
	newNode(protoID string, global nodeIndex, parent edgeIndex, bounded bool) node
}

var _ emitIface = (*PCol[any])(nil)

func (emt *PCol[E]) setPColKey(global nodeIndex, id int, coder any) *pcollectionMetrics {
	emt.valid = true
	emt.globalIndex = global
	emt.localDownstreamIndex = id
	emt.mets = &pcollectionMetrics{nodeIdx: global, nextSampleIdx: 1}
	if coder != nil {
		emt.coder = coder.(coders.Coder[E])
	}
	return emt.mets
}

func (emt *PCol[E]) newDFC(id nodeIndex) processor {
	return &DFC[E]{id: id}
}

func (emt *PCol[E]) newNode(protoID string, global nodeIndex, parent edgeIndex, bounded bool) node {
	return &typedNode[E]{id: protoID, index: global, parentEdge: parent, isBounded: bounded}
}

// Emit the element within the current element's context.
//
// The ElmC value is sourced from the [DFC.Process] method.
func (emt *PCol[E]) Emit(ec ElmC, elm E) {
	// IMPLEMENTATION NOTES:
	// Emit is complicated due to manually inlining PCollection metrics gathering,
	// and calling the downstream processElement function directly.
	// These inlines save measurable per element overhead compared to
	// more ordinary factoring to methods.
	// On a per element per dofn scale, the savings are significant.
	if emt.mets != nil {
		cur := emt.mets.elementCount.Add(1)
		if cur == emt.mets.nextSampleIdx {
			// It's not important for code inside the sampling block here to
			// be inlined since it's run infrequently.
			// TODO move to a helper method?
			if emt.mets.nextSampleIdx < 4 {
				emt.mets.nextSampleIdx++
			} else {
				emt.mets.nextSampleIdx = cur + rand.Int64N(cur/10+2) + 1
			}
			enc := coders.NewEncoder()
			// TODO, optimize this with a sizer instead?
			emt.coder.Encode(enc, elm)
			emt.mets.Sample(int64(len(enc.Data())))
		}
	}
	// Metrics collected, call the downstream function directly to avoid another function layer.
	proc := ec.pcollections[emt.localDownstreamIndex]
	dfc := proc.(*DFC[E])
	dfc.metrics.setState(1, dfc.edgeID) // Set current sampling state.
	if err := dfc.perElm(ElmC{ec.elmContext, dfc.downstream}, elm); err != nil {
		panic(fmt.Errorf("doFn id %v %T failed: %w", dfc.id, dfc.dofn, err))
	}
}

// OnBundleFinish allows a DoFn to register a function that runs just before
// a bundle finishes. Elements may be emitted downstream, if an ElmC is retrieved
// from the DFC.
type OnBundleFinish struct{}

type bundleFinisher interface {
	regBundleFinisher(finishBundle func() error)
}

// Do registers a callback to execute after all bundle elements have been processed.
// Any resources that a DoFn needs explicitly cleaned up explicitly rather than implicitly
// via garbage collection, should be called here.
//
// Only a single callback may be registered, and it will be the last one passed to Do.
func (*OnBundleFinish) Do(dfc bundleFinisher, finishBundle func() error) {
	dfc.regBundleFinisher(finishBundle)
}

////////////////////////////////////////////////////////
// Below here are Not Yet Implemented field flavours. //
////////////////////////////////////////////////////////

// TODO is a marker indicating that a better type should go here at some point
// but it's not yet implemented.
type TODO any

// ObserveWindow indicates this DoFn needs to be aware of windows explicitly.
// Required to use as a field, but may be embedded for legibility.
//
// DoFns that observe windows must process the element for each window individually.
// If ObserveWindow isn't being used, remove it to possibly improve performance.
type ObserveWindow struct{}

// Of returns the window for this element.
func (*ObserveWindow) Of(ec ElmC) TODO { // TODO make this a concrete window type.
	// When windows are observable, only a single window is present.
	return ec.windows[0]
}

// PaneOf returns the window for this element.
func (*ObserveWindow) PaneOf(ec ElmC) TODO { // TODO make this a concrete pane type.
	return ec.pane
}

// AfterBundle allows a DoFn to register a function that runs after
// the bundle has been durably committed. Emitting elements here will fail.
//
// TODO consider moving this to a simple interface function.
// Upside, not likely to try to incorrectly emit in the closure.
// Downside, the caching for anything to finalize needs to be stored in the DoFn struct
// this violates the potential of a ConfigOnly DoFn.
type AfterBundle struct{ beamMixin }

type bundleFinalizer interface {
	regBundleFinalizer(finalizeBundle func() error)
}

// Do registers a func to call after the bundle has been durably committed.
func (*AfterBundle) Do(dfc bundleFinalizer, finalizeBundle func() error) {
	dfc.regBundleFinalizer(finalizeBundle)
}

// OK, so we want to avoid users specifying manual looping, claiming etc. It's a feels bad API.
//
// HOW DO WE AVOID THE FEELS BAD?
// We need to have it so the user is authoring something discoverable.
// We need to avoid giving the user the tracker, but enable what the user needs a tracker for.

// Restriction is a range of logical positions to be processed for this element.
// Restriction implementations must be serializable.
type Restriction[P any] interface {
	// Start is the earliest position in this restriction.
	Start() P
	// End is the last position that must be processed with this restriction.
	End() P
	// Bounded whether this restiction is bounded or not.
	Bounded() bool
}

// Tracker manages state around splitting an element.
//
// Tracker implementations are not serialized.
type Tracker[R Restriction[P], P any] interface {
	// Size returns a an estimate of the amount of work in this restrction.
	// A zero size restriction isn't permitted.
	Size(R) float64
	// TryClaim attempts to claim the given position within the restriction.
	// Claiming a position at or beyond the end of the restriction signals that the
	// entire restriction has been processed and is now done, at which point this
	// method signals to end processing.
	TryClaim(P) bool

	// TrySplit splits at the nearest position greater than the given fraction of the remainder. If the
	// fraction given is outside of the position's range, it is clamped to Min or Max.
	TrySplit(fraction float64) (primary, residual R, err error)
	IsDone() bool
	GetError() error
	GetProgress() (done, remaining float64)
	GetRestriction() R
}

// lockingTracker wraps a Tracker in a mutex to synchronize access.
type lockingTracker[T Tracker[R, P], R Restriction[P], P any] struct {
	mu      *sync.Mutex
	wrapped T
}

func wrapWithLockTracker[T Tracker[R, P], R Restriction[P], P any](t T, mu *sync.Mutex) *lockingTracker[T, R, P] {
	return &lockingTracker[T, R, P]{mu: mu, wrapped: t}
}

func (t *lockingTracker[T, R, P]) Size(rest R) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.Size(rest)
}

func (t *lockingTracker[T, R, P]) TryClaim(pos P) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.TryClaim(pos)
}

func (t *lockingTracker[T, R, P]) TrySplit(fraction float64) (R, R, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.TrySplit(fraction)
}

func (t *lockingTracker[T, R, P]) GetError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.GetError()
}

func (t *lockingTracker[T, R, P]) GetRestriction() R {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.GetRestriction()
}

func (t *lockingTracker[T, R, P]) GetProgress() (done float64, remaining float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.GetProgress()
}

func (t *lockingTracker[T, R, P]) IsDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.wrapped.IsDone()
}

// TryClaim processes a DoFn provided closure, passing in a claimed position.
// The closure returns the next position, or an error.
type TryClaim[P any] func(func(P) (P, error)) error

// ProcessRestriction defines processing the given element with respect to the provided
// restriction.
type ProcessRestriction[E any, R Restriction[P], P any] func(ElmC, E, R, TryClaim[P]) error

// BoundedSDF indicates this DoFn is able to split elements into independently
// processessable sub parts, called Restrictions.
//
// Due to the handling required, call the BoundedSDF [Process] method, instead
// of the one on DFC.
type BoundedSDF[FAC RestrictionFactory[E, R, P], E any, T Tracker[R, P], R Restriction[P], P, WES any] struct{}

// Process is called during ProcessBundle set up to define the processing happening per element.
func (sdf BoundedSDF[FAC, E, T, R, P, WES]) Process(dfc *DFC[E],
	makeTracker func(R) T,
	proc ProcessRestriction[E, R, P]) error {

	dfc.makeTracker = makeTracker
	dfc.perElmAndRest = proc
	return nil
}

// AddRestrictionCoder provides the id of the coder for the restriction type.
func (sdf BoundedSDF[FAC, E, T, R, P, WES]) addRestrictionCoder(intern map[string]string, coders map[string]*pipepb.Coder) string {
	// The WatermarkEstimator state is propagated with the Restrictions.
	return addCoder[KV[R, WES]](intern, coders)
}

func (sdf BoundedSDF[FAC, E, T, R, P, WES]) pairWithRestriction() any {
	return &pairWithRestriction[FAC, E, R, P, WES]{}
}

func (sdf BoundedSDF[FAC, E, T, R, P, WES]) splitAndSizeRestriction() any {
	return &splitAndSizeRestrictions[FAC, E, R, P, WES]{}
}

func (sdf BoundedSDF[FAC, E, T, R, P, WES]) processSizedElementAndRestriction(userDoFn any, coders map[string]*pipepb.Coder, coderID, tid, inputID string) any {
	return &processSizedElementAndRestriction[FAC, E, T, R, P, WES]{
		Transform:        userDoFn.(Transform[E]),
		fullElementCoder: coderFromProto[KV[KV[E, KV[R, WES]], float64]](coders, coderID),
		tid:              tid,
		inputID:          inputID,
	}
}

type sdfHandler interface {
	addRestrictionCoder(intern map[string]string, coders map[string]*pipepb.Coder) string
	pairWithRestriction() any
	splitAndSizeRestriction() any
	processSizedElementAndRestriction(userDoFn any, coders map[string]*pipepb.Coder, elementCoderID, tid, inputID string) any
}

var (
	_ sdfHandler = BoundedSDF[RestrictionFactory[int, Restriction[int], int], int, Tracker[Restriction[int], int], Restriction[int], int, int]{}
)

// Marker methods for BoundedSDF for type extraction? Also for handling splits?

// TODO Watermark Estimators and ProcessContinuations for StreamingDoFn

//////////////////////
// State and Timers //
//////////////////////

type timer struct{ beamMixin }

func (timer) timerIface() {}

type timerIface interface {
	timerIface()
}

type TimerEvent struct{ timer }
type TimerProcessing struct{ timer }

var (
	_ timerIface = (*TimerEvent)(nil)
	_ timerIface = (*TimerProcessing)(nil)
)

// what else am I missing?
//
// Error and panic propagation.
//
// Triggers, Windowing, CustomWindowFn,
// Metrics
// GroupIntoBatches (With Sharded Key)
// CoGBK
//
//  CoCombine?
//
// Preserve Keys, Observe Keys
//
// DisplayData, Annotations
//
// DoFn Sampler and State Caching
//
// logging is slog.

// Notes for later Axel Wagner talk on Advanced Generics.
// Type constraint to *only* pointer type, of some interface types.
// type foo[T any] interface {
// 	*T
// 	// other interface, eg json.Unmarshaller
// }

// Phantom types.
// type mykey[T any] struct{}

// use as a key into maps of interface types.
// Useful for type based state instead of using reflect.TypeOf
// Use phantom typed maps for registries.

// type endpoint[Req, Resp any] string
// Define specific things.
// But define vars instead of consts for specific instances.
// func Call[Req, Resp any](c *Client, e endpoint[Req, Resp], r Req) (Resp, error)
//
// Use unnamed fields but typed. Allows type safety and prevents user misuse by casting etc.
// type endpont[Req, Resp any] struct{ _ [0]Req; _ [0]Resp; name string }
