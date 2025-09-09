package beam

import (
	"context"
	"io"
	"iter"
	"maps"
	"reflect"
	"slices"

	"github.com/go-json-experiment/json"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// State in Beam is associated with a window, and a key, and a DoFn.
// This file defines the different kinds of state that Beam can maintain.

// state is a zero sized mixin to be embedded into valid state types as fields.
type state struct{ beamMixin }

func (state) isState()       {}
func (state) persist() error { panic("persist unimplemented") }

type stateIface interface {
	isState()
	persist() error
	toProtoParts(params translateParams) *pipepb.StateSpec
	initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder)
}

const (
	urnBagUserState      = "beam:user_state:bag:v1"
	urnMultiMapUserState = "beam:user_state:multimap:v1"
)

type (
	// In an ideal world, we'd have a single place where we know the state type
	// and the Key, the Window, and the *state's* element type statically.
	// Unfortunately, we do not, as the state fields are ultimately disassociated
	// with the wrapper DoFn's types, and it would be inconvenient to users to
	// force it otherwise.
	//
	// So, the choice is between typed Keys and windows, and Typed elements.
	//
	// We will go with typed elements for the reason that they are likely more expensive
	// to always encode/decode (especially for the multi-value states), while
	// we will almost always need the key and window encodings.
	//
	// This means each state needs it's own K+W cache cells, and we need to pass
	// this information down to the states via the ElmC.
	stateCacheKey struct {
		k, w string
		// TODO change window value to the true window type?
	}

	// stateCacheEntry represents the consolidated view of the cache and whether
	// anything has been loaded from the runner.
	//
	// This is because we must avoid duplicating values in multi-value states,
	// but also strive for efficiency in StateAPI calls.
	// fresh is the values that have been produced during this bundle invocation.
	// The marker values track what might need doing on bundle finish for writing
	// to the state efficiently.
	//
	// Initial state is that there is no state, and everything is zeroed.
	//
	// Marker Booleans
	//
	// valid indicates that this entry has been written to in this bundle, and
	// it contains real values.
	//
	// dirty indicates that the fresh value is valid. This will often be
	// returned with a user read call, disjuncted with loaded as needed.
	//
	// loaded indicates that the runner field is valid, and that we have already
	// read the state value from the runner.
	//
	// cleared indicates that the state was cleared at least once, and thus
	// we must clear the state runner side, before sending the fresh value over.
	//
	// We could probably simplify some of this book keeping by always sending
	// a clear before appending, but that would be a problem for larger, more
	// expensive to encode values.
	stateCacheEntry[V any] struct {
		fresh, runner                 V
		valid, dirty, loaded, cleared bool
	}

	// stateCache handles per bundle state within a state cell.
	stateCache[V any] struct {
		cache map[stateCacheKey]stateCacheEntry[V]
	}
)

func newStateCache[V any]() *stateCache[V] {
	return &stateCache[V]{
		cache: map[stateCacheKey]stateCacheEntry[V]{},
	}
}

// Get returns the cached state, and the valid bit, and exists bit.
//
// The valid bit is whether the state has been cleared.
// and that the value must be persisted to the runner eventually.
//
// A false exists, means there's no work to be done for this KV pair.
func (sc *stateCache[V]) Get(k, w string) stateCacheEntry[V] {
	return sc.cache[stateCacheKey{k, w}]
}

// Put replaces the state in the cache, and sets the valid bit to true.
func (sc *stateCache[V]) Put(k, w string, entry stateCacheEntry[V]) {
	entry.valid = true
	sc.cache[stateCacheKey{k, w}] = entry
}

// Clear zeroes the state in the cache, and sets the valid bit to false, and
// the cleared bit to true.
func (sc *stateCache[V]) Clear(k, w string) {
	sc.cache[stateCacheKey{k, w}] = stateCacheEntry[V]{valid: false, dirty: true, cleared: true}
}

// Iter provides iteration through the cache, but also clears the cache immeadiately.
// Intended for permitting a final dumping/reading of the state for persistence
// at the end of the bundle.
func (sc *stateCache[V]) Iter() iter.Seq2[stateCacheKey, stateCacheEntry[V]] {
	defer func() { sc.cache = map[stateCacheKey]stateCacheEntry[V]{} }()
	return maps.All(sc.cache)
}

// StateBag represents an unordered collection of state associated with the
// embedded DoFn, the element's window, and a key.
type StateBag[E Element] struct {
	state

	initBagReader   func(key, win []byte) harness.NextBuffer
	initBagAppender func(key, win []byte) io.Writer
	initBagClearer  func(key, win []byte) io.Writer
	coder           coders.Coder[E]

	cache *stateCache[[]E]
}

var _ stateIface = (*StateBag[int])(nil)

func (st *StateBag[E]) toProtoParts(params translateParams) *pipepb.StateSpec {
	coderID := addCoder[E](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_BagSpec{
			BagSpec: &pipepb.BagStateSpec{
				ElementCoderId: coderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnBagUserState,
		},
	}
}

func (st *StateBag[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetBagSpec().GetElementCoderId()
	st.coder = coderFromProto[E](coders, coderID)

	keyPBFn := func(key, win []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_BagUserState_{
				BagUserState: &fnpb.StateKey_BagUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
				},
			},
		}
	}
	st.initBagReader = func(key, win []byte) harness.NextBuffer {
		keyPb := keyPBFn(key, win)
		// 50/50 on putting this on processor directly instead??
		r, err := dataCon.State.OpenReader(ctx, url, keyPb)
		if err != nil {
			panic(err)
		}
		return r
	}
	st.initBagAppender = func(key, win []byte) io.Writer {
		keyPb := keyPBFn(key, win)
		w, err := dataCon.State.OpenWriter(ctx, url, keyPb, harness.StateWriteAppend)
		if err != nil {
			panic(err)
		}
		return w
	}
	st.initBagClearer = func(key, win []byte) io.Writer {
		keyPb := keyPBFn(key, win)
		w, err := dataCon.State.OpenWriter(ctx, url, keyPb, harness.StateWriteClear)
		if err != nil {
			panic(err)
		}
		return w
	}

	st.cache = newStateCache[[]E]()
}

func (st *StateBag[E]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid {
			continue
		}

		if entry.cleared {
			c := st.initBagClearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		// We only ever need to write the fresh values for the bag.
		w := st.initBagAppender([]byte(key.k), []byte(key.w))
		enc := coders.NewEncoder()
		for _, v := range entry.fresh {
			st.coder.Encode(enc, v)
		}
		if _, err := w.Write(enc.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (st *StateBag[E]) Append(ec ElmC, val E) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	entry.fresh = append(entry.fresh, val)
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

func (st *StateBag[E]) Clear(ec ElmC) {
	st.cache.Clear(ec.keyBytes, ec.winBytes)
}

func (st *StateBag[E]) Read(ec ElmC) iter.Seq[E] {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if !entry.valid {
		// We have no local elements, so just return the iterator immeadiately.
		r := st.initBagReader([]byte(ec.keyBytes), []byte(ec.winBytes))
		iter := iterClosureWithCoder(st.coder, r)

		// TODO: Do smarter handling for large amounts of state.
		for val := range iter {
			entry.runner = append(entry.runner, val)
		}
		entry.loaded = true
		st.cache.Put(ec.keyBytes, ec.winBytes, entry)
		return slices.Values(entry.runner)
	}
	return concat(slices.Values(entry.runner), slices.Values(entry.fresh))
}

// StateValue represents a single value of state associated with the
// embedded DoFn, the element's window, and a key.
type StateValue[E Element] struct {
	state
	coder coders.Coder[E]

	initBagReader   func(key, win []byte) harness.NextBuffer
	initBagAppender func(key, win []byte) io.Writer
	initBagClearer  func(key, win []byte) io.Writer

	cache *stateCache[E]
}

var _ stateIface = (*StateValue[int])(nil)

func (st *StateValue[E]) toProtoParts(params translateParams) *pipepb.StateSpec {
	coderID := addCoder[E](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_ReadModifyWriteSpec{
			ReadModifyWriteSpec: &pipepb.ReadModifyWriteStateSpec{
				CoderId: coderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnBagUserState,
		},
	}
}

func (st *StateValue[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetReadModifyWriteSpec().GetCoderId()
	st.coder = coderFromProto[E](coders, coderID)

	keyPBFn := func(key, win []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_BagUserState_{
				BagUserState: &fnpb.StateKey_BagUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
				},
			},
		}
	}
	st.initBagReader = func(key, win []byte) harness.NextBuffer {
		keyPb := keyPBFn(key, win)
		// 50/50 on putting this on processor directly instead??
		r, err := dataCon.State.OpenReader(ctx, url, keyPb)
		if err != nil {
			panic(err)
		}
		return r
	}
	st.initBagAppender = func(key, win []byte) io.Writer {
		keyPb := keyPBFn(key, win)
		w, err := dataCon.State.OpenWriter(ctx, url, keyPb, harness.StateWriteAppend)
		if err != nil {
			panic(err)
		}
		return w
	}
	st.initBagClearer = func(key, win []byte) io.Writer {
		keyPb := keyPBFn(key, win)
		w, err := dataCon.State.OpenWriter(ctx, url, keyPb, harness.StateWriteClear)
		if err != nil {
			panic(err)
		}
		return w
	}

	st.cache = newStateCache[E]()
}

func (st *StateValue[E]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid {
			continue
		}

		if entry.cleared {
			c := st.initBagClearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		w := st.initBagAppender([]byte(key.k), []byte(key.w))
		enc := coders.NewEncoder()
		st.coder.Encode(enc, entry.fresh)
		if _, err := w.Write(enc.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (st *StateValue[E]) Set(ec ElmC, val E) {
	entry := stateCacheEntry[E]{
		fresh:   val,
		cleared: true, // All Sets requires clearing the runner state.
	}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

func (st *StateValue[E]) Clear(ec ElmC) {
	st.cache.Clear(ec.keyBytes, ec.winBytes)
}

func (st *StateValue[E]) Read(ec ElmC) (E, bool) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if entry.valid {
		return entry.fresh, true
	}
	// Nothing cached, so we must read.
	r := st.initBagReader([]byte(ec.keyBytes), []byte(ec.winBytes))
	iter := iterClosureWithCoder(st.coder, r)

	iter(func(in E) bool {
		entry.fresh = in
		entry.valid = true
		return false
	})

	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
	return entry.fresh, entry.valid
}

// AsStateCombining uses a [Combiner] to produce
func AsStateCombining[A, I, O Element, AM AccumulatorMerger[A]](comb Combiner[A, I, O, AM]) StateCombining[A, I, O, AM] {
	return StateCombining[A, I, O, AM]{
		comb: comb,
	}
}

// StateCombining represents an accumulator value and a combining function
// associated with the embedded DoFn, the element's window, and a key.
//
// Must be created using AsStateCombining, using a standard combiner.
type StateCombining[A, I, O Element, AM AccumulatorMerger[A]] struct {
	state
	comb  Combiner[A, I, O, AM]
	coder coders.Coder[A]
}

var _ stateIface = (*StateCombining[int, int, int, AccumulatorMerger[int]])(nil)

func (st *StateCombining[A, I, O, AM]) accessPatternUrn() string {
	return urnBagUserState
}

func (st *StateCombining[A, I, O, AM]) toProtoParts(params translateParams) *pipepb.StateSpec {
	coderID := addCoder[A](params.InternedCoders, params.Comps.GetCoders())

	rv := reflect.ValueOf(st.comb.am)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	// Register types with the lookup table.
	typeName := rv.Type().Name()
	params.TypeReg[typeName] = rv.Type()

	wrap := dofnWrap{
		TypeName: typeName,
		DoFn:     st.comb.am,
	}
	wrappedPayload, err := json.Marshal(&wrap, json.DefaultOptionsV2(), jsonDoFnMarshallers())
	if err != nil {
		panic(err)
	}

	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_CombiningSpec{
			CombiningSpec: &pipepb.CombiningStateSpec{
				AccumulatorCoderId: coderID,
				CombineFn: &pipepb.FunctionSpec{
					Urn:     "beam:gosdk:state:combinefn:v1",
					Payload: wrappedPayload,
				},
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnBagUserState,
		},
	}
}

func (st *StateCombining[A, I, O, AM]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetCombiningSpec().GetAccumulatorCoderId()
	st.coder = coderFromProto[A](coders, coderID)
	panic("unimplemented")
}

// StateMap represents a single key, value store
// associated with the embedded DoFn, the element's window, and a key.
type StateMap[K Keys, V Element] struct {
	state

	keyCoder coders.Coder[K]
	valCoder coders.Coder[V]
}

var _ stateIface = (*StateMap[int, int])(nil)

func (st *StateMap[K, V]) toProtoParts(params translateParams) *pipepb.StateSpec {
	keyCoderID := addCoder[K](params.InternedCoders, params.Comps.GetCoders())
	valueCoderID := addCoder[V](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_MapSpec{
			MapSpec: &pipepb.MapStateSpec{
				KeyCoderId:   keyCoderID,
				ValueCoderId: valueCoderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnMultiMapUserState,
		},
	}
}

func (st *StateMap[K, V]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	keyCoderID := spec.GetMapSpec().GetKeyCoderId()
	st.keyCoder = coderFromProto[K](coders, keyCoderID)

	valCoderID := spec.GetMapSpec().GetKeyCoderId()
	st.valCoder = coderFromProto[V](coders, valCoderID)
	panic("unimplemented")
}

// StateSet represents a de-duplicated set of values
// associated with the embedded DoFn, the element's window, and a key.
//
// Values are deduplicated by their encoded value.
type StateSet[E Element] struct {
	state
	coder coders.Coder[E]
}

var _ stateIface = (*StateSet[int])(nil)

func (st *StateSet[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetSetSpec().GetElementCoderId()
	st.coder = coderFromProto[E](coders, coderID)
	panic("unimplemented")
}

func (st *StateSet[E]) toProtoParts(params translateParams) *pipepb.StateSpec {
	coderID := addCoder[E](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_SetSpec{
			SetSpec: &pipepb.SetStateSpec{
				ElementCoderId: coderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnMultiMapUserState,
		},
	}
}

// StateMultiMap represents a mapping of keys to lists of values.
type StateMultiMap[K Keys, V Element] struct {
	state

	keyCoder coders.Coder[K]
	valCoder coders.Coder[V]
}

var _ stateIface = (*StateMultiMap[int, int])(nil)

func (st *StateMultiMap[K, V]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	keyCoderID := spec.GetMultimapSpec().GetKeyCoderId()
	st.keyCoder = coderFromProto[K](coders, keyCoderID)

	valCoderID := spec.GetMultimapSpec().GetKeyCoderId()
	st.valCoder = coderFromProto[V](coders, valCoderID)
	panic("unimplemented")
}

func (st *StateMultiMap[K, V]) toProtoParts(params translateParams) *pipepb.StateSpec {
	keyCoderID := addCoder[K](params.InternedCoders, params.Comps.GetCoders())
	valueCoderID := addCoder[V](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_MultimapSpec{
			MultimapSpec: &pipepb.MultimapStateSpec{
				KeyCoderId:   keyCoderID,
				ValueCoderId: valueCoderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnBagUserState,
		},
	}
}

// StateOrderedList represents a sorted list of values, ordered by event time.
// associated with the embedded DoFn, the element's window, and a key.
type StateOrderedList[E Element] struct {
	state

	coder coders.Coder[E]
}

var _ stateIface = (*StateOrderedList[int])(nil)

func (st *StateOrderedList[E]) initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetOrderedListSpec().GetElementCoderId()
	st.coder = coderFromProto[E](coders, coderID)
}

func (st *StateOrderedList[E]) toProtoParts(params translateParams) *pipepb.StateSpec {
	// TODO: Do the correct coder, including the event time in the encoding.
	coderID := addCoder[E](params.InternedCoders, params.Comps.GetCoders())
	return &pipepb.StateSpec{
		Spec: &pipepb.StateSpec_OrderedListSpec{
			OrderedListSpec: &pipepb.OrderedListStateSpec{
				ElementCoderId: coderID,
			},
		},
		Protocol: &pipepb.FunctionSpec{
			Urn: urnBagUserState,
		},
	}
}
