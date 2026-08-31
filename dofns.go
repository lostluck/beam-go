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
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
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
	// Metrics collected, call the downstream processElement function to handle window explosion.
	// // TODO investigate if we can avoid the extra function layer.
	proc := ec.pcollections[emt.localDownstreamIndex]
	dfc := proc.(*DFC[E])
	dfc.metrics.setState(1, dfc.edgeID) // Set current sampling state.
	if err := dfc.processElement(ElmC{ec.elmContext, dfc.downstream}, elm); err != nil {
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

// windowObserver is implemented by DoFn fields or transforms that explicitly observe the window or pane.
type windowObserver interface {
	observesWindow()
}

// ObserveWindow indicates this DoFn needs to be aware of windows explicitly.
// Required to use as a field, but may be embedded for legibility.
//
// When ObserveWindow is used, elements are processed per window (window explosion).
type ObserveWindow[W window.BoundedWindow] struct {
	beamMixin
}

func (ObserveWindow[W]) observesWindow() {}

// Of returns the window for this element.
func (*ObserveWindow[W]) Of(ec ElmC) W {
	var win window.BoundedWindow
	if ec.window != nil {
		win = ec.window
	} else if len(ec.windows) > 0 {
		win = ec.windows[0]
	} else {
		win = window.GlobalWindow{}
	}
	if typedWin, ok := win.(W); ok {
		return typedWin
	}
	var zero W
	return zero
}

// PaneOf returns the pane for this element.
func (*ObserveWindow[W]) PaneOf(ec ElmC) coders.PaneInfo {
	return ec.pane
}

// ObservePane allows observing pane metadata for an element.
type ObservePane struct {
	beamMixin
}

func (ObservePane) observesWindow() {}

// Of returns the pane metadata for this element.
func (*ObservePane) Of(ec ElmC) coders.PaneInfo {
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

type timerIface interface {
	isTimer()
	timeDomain() pipepb.TimeDomain_Enum
	toProtoParts(params translateParams) *pipepb.TimerFamilySpec
	initialize(tmb *timerInitBase, familyID, transformID string, spec *pipepb.TimerFamilySpec, coders map[string]*pipepb.Coder)
	writeTimers(ctx context.Context) error
	setFamilyID(id string)
	getFamilyID() string
}

type windowExpiryIface interface {
	timerIface
	isWindowExpiry()
}

type timerRegistrar interface {
	registerTimerCallback(familyID string, cb func(ec ElmC, key any, tag string) error)
}

type timer struct {
	beamMixin
	familyID string
	init     *timerInitBase
	pending  []pendingTimer
}

type pendingTimer struct {
	keyBytes      string
	winBytes      string
	tag           string
	clearBit      bool
	fireTimestamp time.Time
	holdTimestamp time.Time
	pane          coders.PaneInfo
}

func (t *timer) isTimer() {}

func (t *timer) setFamilyID(id string) {
	t.familyID = id
}

func (t *timer) getFamilyID() string {
	return t.familyID
}

func (t *timer) initialize(tmb *timerInitBase, familyID, transformID string, spec *pipepb.TimerFamilySpec, coders map[string]*pipepb.Coder) {
	t.init = tmb
	t.familyID = familyID
	t.pending = nil
}

func (t *timer) writeTimers(ctx context.Context) error {
	if len(t.pending) == 0 || t.init == nil {
		return nil
	}
	defer func() { t.pending = nil }()

	w, err := t.init.dataCon.Data.OpenTimerWrite(ctx, harness.StreamID{PtransformID: t.init.transformID}, t.familyID)
	if err != nil {
		return err
	}

	for _, pt := range t.pending {
		enc := coders.NewEncoder()
		copy(enc.Grow(len(pt.keyBytes)), pt.keyBytes)
		enc.TimerHeader(pt.tag, [][]byte{[]byte(pt.winBytes)}, pt.clearBit)
		if !pt.clearBit {
			enc.TimerDetails(pt.fireTimestamp, pt.holdTimestamp, pt.pane)
		}
		if _, err := w.Write(enc.Data()); err != nil {
			_ = w.Close()
			return err
		}
	}
	return w.Close()
}

// TimerEvent is an event-time timer field on a Stateful DoFn.
// It fires when the watermark advances past the set timestamp.
type TimerEvent[K Keys] struct{ timer }

func (TimerEvent[K]) timeDomain() pipepb.TimeDomain_Enum {
	return pipepb.TimeDomain_EVENT_TIME
}

func (t *TimerEvent[K]) toProtoParts(params translateParams) *pipepb.TimerFamilySpec {
	keyCoderID := addCoder[K](params.InternedCoders, params.Comps.GetCoders())
	winCoderID := params.WindowCoderID
	if winCoderID == "" {
		winCoderID = "gwc"
	}
	timerCoderID := putCoder(params.Comps.GetCoders(), "beam:coder:timer:v1", nil, []string{keyCoderID, winCoderID})
	return &pipepb.TimerFamilySpec{
		TimeDomain:         pipepb.TimeDomain_EVENT_TIME,
		TimerFamilyCoderId: timerCoderID,
	}
}

func (t *TimerEvent[K]) Set(ec ElmC, targetTime time.Time) {
	t.SetWithTag(ec, "", targetTime)
}

func (t *TimerEvent[K]) SetWithTag(ec ElmC, tag string, targetTime time.Time) {
	t.pending = append(t.pending, pendingTimer{
		keyBytes:      ec.keyBytes,
		winBytes:      ec.winBytes,
		tag:           tag,
		clearBit:      false,
		fireTimestamp: targetTime,
		holdTimestamp: targetTime,
		pane:          ec.pane,
	})
}

func (t *TimerEvent[K]) Clear(ec ElmC) {
	t.ClearTag(ec, "")
}

func (t *TimerEvent[K]) ClearTag(ec ElmC, tag string) {
	t.pending = append(t.pending, pendingTimer{
		keyBytes: ec.keyBytes,
		winBytes: ec.winBytes,
		tag:      tag,
		clearBit: true,
	})
}

func (t *TimerEvent[K]) OnFire(dfc timerRegistrar, callback func(ec ElmC, key K) error) {
	dfc.registerTimerCallback(t.familyID, func(ec ElmC, key any, tag string) error {
		return callback(ec, key.(K))
	})
}

func (t *TimerEvent[K]) OnFireTagged(dfc timerRegistrar, callback func(ec ElmC, key K, tag string) error) {
	dfc.registerTimerCallback(t.familyID, func(ec ElmC, key any, tag string) error {
		return callback(ec, key.(K), tag)
	})
}

// TimerProcessing is a processing-time timer field on a Stateful DoFn.
// It fires when real-time processing time advances past the set timestamp.
type TimerProcessing[K Keys] struct{ timer }

func (TimerProcessing[K]) timeDomain() pipepb.TimeDomain_Enum {
	return pipepb.TimeDomain_PROCESSING_TIME
}

func (t *TimerProcessing[K]) toProtoParts(params translateParams) *pipepb.TimerFamilySpec {
	keyCoderID := addCoder[K](params.InternedCoders, params.Comps.GetCoders())
	winCoderID := params.WindowCoderID
	if winCoderID == "" {
		winCoderID = "gwc"
	}
	timerCoderID := putCoder(params.Comps.GetCoders(), "beam:coder:timer:v1", nil, []string{keyCoderID, winCoderID})
	return &pipepb.TimerFamilySpec{
		TimeDomain:         pipepb.TimeDomain_PROCESSING_TIME,
		TimerFamilyCoderId: timerCoderID,
	}
}

func (t *TimerProcessing[K]) Set(ec ElmC, targetTime time.Time) {
	t.SetWithTag(ec, "", targetTime)
}

func (t *TimerProcessing[K]) SetWithTag(ec ElmC, tag string, targetTime time.Time) {
	t.pending = append(t.pending, pendingTimer{
		keyBytes:      ec.keyBytes,
		winBytes:      ec.winBytes,
		tag:           tag,
		clearBit:      false,
		fireTimestamp: targetTime,
		holdTimestamp: targetTime,
		pane:          ec.pane,
	})
}

func (t *TimerProcessing[K]) Clear(ec ElmC) {
	t.ClearTag(ec, "")
}

func (t *TimerProcessing[K]) ClearTag(ec ElmC, tag string) {
	t.pending = append(t.pending, pendingTimer{
		keyBytes: ec.keyBytes,
		winBytes: ec.winBytes,
		tag:      tag,
		clearBit: true,
	})
}

func (t *TimerProcessing[K]) OnFire(dfc timerRegistrar, callback func(ec ElmC, key K) error) {
	dfc.registerTimerCallback(t.familyID, func(ec ElmC, key any, tag string) error {
		return callback(ec, key.(K))
	})
}

func (t *TimerProcessing[K]) OnFireTagged(dfc timerRegistrar, callback func(ec ElmC, key K, tag string) error) {
	dfc.registerTimerCallback(t.familyID, func(ec ElmC, key any, tag string) error {
		return callback(ec, key.(K), tag)
	})
}

// OnWindowExpiration is an optional field on a Stateful DoFn that is called
// when a window expires past allowed lateness, allowing the DoFn to flush, emit,
// or process any remaining buffered state before the window is discarded.
type OnWindowExpiration[K Keys] struct{ timer }

func (OnWindowExpiration[K]) timeDomain() pipepb.TimeDomain_Enum {
	return pipepb.TimeDomain_EVENT_TIME
}

func (OnWindowExpiration[K]) isWindowExpiry() {}

func (w *OnWindowExpiration[K]) toProtoParts(params translateParams) *pipepb.TimerFamilySpec {
	keyCoderID := addCoder[K](params.InternedCoders, params.Comps.GetCoders())
	winCoderID := params.WindowCoderID
	if winCoderID == "" {
		winCoderID = "gwc"
	}
	timerCoderID := putCoder(params.Comps.GetCoders(), "beam:coder:timer:v1", nil, []string{keyCoderID, winCoderID})
	return &pipepb.TimerFamilySpec{
		TimeDomain:         pipepb.TimeDomain_EVENT_TIME,
		TimerFamilyCoderId: timerCoderID,
	}
}

func (w *OnWindowExpiration[K]) OnExpire(dfc timerRegistrar, callback func(ec ElmC, key K) error) {
	dfc.registerTimerCallback(w.familyID, func(ec ElmC, key any, tag string) error {
		return callback(ec, key.(K))
	})
}

var (
	_ timerIface        = (*TimerEvent[int])(nil)
	_ timerIface        = (*TimerProcessing[int])(nil)
	_ windowExpiryIface = (*OnWindowExpiration[int])(nil)
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
