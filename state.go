package beam

import (
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"reflect"
	"slices"
	"time"

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
	initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder)
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
		fresh, runner          V
		valid, loaded, cleared bool
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
	sc.cache[stateCacheKey{k, w}] = stateCacheEntry[V]{valid: false, cleared: true}
}

// Iter provides iteration through the cache, but also clears the cache immeadiately.
// Intended for permitting a final dumping/reading of the state for persistence
// at the end of the bundle.
func (sc *stateCache[V]) Iter() iter.Seq2[stateCacheKey, stateCacheEntry[V]] {
	defer func() { sc.cache = map[stateCacheKey]stateCacheEntry[V]{} }()
	return maps.All(sc.cache)
}

type stateInitBase struct {
	ctx     context.Context
	dataCon harness.DataContext
	url     string
}

type stateInit struct {
	*stateInitBase
	keyPBFn func(key, win []byte) *fnpb.StateKey
}

func (stib *stateInitBase) reader(stateKey *fnpb.StateKey) harness.NextBuffer {
	r, err := stib.dataCon.State.OpenReader(stib.ctx, stib.url, stateKey)
	if err != nil {
		panic(err)
	}
	return r
}

func (stib *stateInitBase) appender(stateKey *fnpb.StateKey) io.Writer {
	w, err := stib.dataCon.State.OpenWriter(stib.ctx, stib.url, stateKey, harness.StateWriteAppend)
	if err != nil {
		panic(err)
	}
	return w
}

func (stib *stateInitBase) clearer(stateKey *fnpb.StateKey) io.Writer {
	w, err := stib.dataCon.State.OpenWriter(stib.ctx, stib.url, stateKey, harness.StateWriteClear)
	if err != nil {
		panic(err)
	}
	return w
}

func (sti stateInit) Reader(key, win []byte) harness.NextBuffer {
	keyPb := sti.keyPBFn(key, win)
	return sti.reader(keyPb)
}

func (sti stateInit) Appender(key, win []byte) io.Writer {
	keyPb := sti.keyPBFn(key, win)
	return sti.appender(keyPb)
}

func (sti stateInit) Clearer(key, win []byte) io.Writer {
	keyPb := sti.keyPBFn(key, win)
	return sti.clearer(keyPb)
}

type stateMapInit struct {
	*stateInitBase
	valsPBFn func(key, win, user []byte) *fnpb.StateKey
}

func (sti stateMapInit) Reader(key, win, user []byte) harness.NextBuffer {
	stateKey := sti.valsPBFn(key, win, user)
	return sti.reader(stateKey)
}

func (sti stateMapInit) Appender(key, win, user []byte) io.Writer {
	stateKey := sti.valsPBFn(key, win, user)
	return sti.appender(stateKey)
}

func (sti stateMapInit) Clearer(key, win, user []byte) io.Writer {
	stateKey := sti.valsPBFn(key, win, user)
	return sti.clearer(stateKey)
}

// StateBag represents an unordered collection of state associated with the
// embedded DoFn, the element's window, and a key.
type StateBag[E Element] struct {
	state
	init  stateInit
	coder coders.Coder[E]

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

func (st *StateBag[E]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
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
	st.init = stateInit{
		keyPBFn:       keyPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[[]E]()
}

func (st *StateBag[E]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid {
			continue
		}

		if entry.cleared {
			c := st.init.Clearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		// We only ever need to write the fresh values for the bag.
		w := st.init.Appender([]byte(key.k), []byte(key.w))
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
		r := st.init.Reader([]byte(ec.keyBytes), []byte(ec.winBytes))
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

	init stateInit

	coder coders.Coder[E]
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

func (st *StateValue[E]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
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

	st.init = stateInit{
		keyPBFn:       keyPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[E]()
}

func (st *StateValue[E]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid {
			continue
		}

		if entry.cleared {
			c := st.init.Clearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		w := st.init.Appender([]byte(key.k), []byte(key.w))
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

func (st *StateValue[E]) Get(ec ElmC) (E, bool) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if entry.valid {
		return entry.fresh, true
	}
	// Nothing cached, so we must read.
	r := st.init.Reader([]byte(ec.keyBytes), []byte(ec.winBytes))
	iter := iterClosureWithCoder(st.coder, r)

	iter(func(in E) bool {
		entry.fresh = in
		entry.valid = true
		return false
	})

	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
	return entry.fresh, entry.valid
}

// StateMap represents a single key, value store
// associated with the embedded DoFn, the element's window, and a key.
type StateMap[K Keys, V Element] struct {
	state
	initKeys stateInit // Can only use Read and Clear.
	initVals stateMapInit

	keyCoder coders.Coder[K]
	valCoder coders.Coder[V]

	cache *stateCache[map[K]stateCacheEntry[V]]
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

func (st *StateMap[K, V]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	keyCoderID := spec.GetMapSpec().GetKeyCoderId()
	st.keyCoder = coderFromProto[K](coders, keyCoderID)

	valCoderID := spec.GetMapSpec().GetValueCoderId()
	st.valCoder = coderFromProto[V](coders, valCoderID)

	valuesPBFn := func(key, win, user []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapUserState_{
				MultimapUserState: &fnpb.StateKey_MultimapUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
					MapKey:      user,
				},
			},
		}
	}
	keysPBFn := func(key, win []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapKeysUserState_{
				MultimapKeysUserState: &fnpb.StateKey_MultimapKeysUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
				},
			},
		}
	}

	st.initKeys = stateInit{
		keyPBFn:       keysPBFn,
		stateInitBase: stb,
	}

	st.initVals = stateMapInit{
		valsPBFn:      valuesPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[map[K]stateCacheEntry[V]]()
}

func (st *StateMap[K, V]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid {
			panic(fmt.Sprintf("state %+v has an invalid entry %+v", key, entry))
		}

		if entry.cleared {
			c := st.initKeys.Clearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		for userKey, userEntry := range entry.fresh {
			if !userEntry.valid {
				panic(fmt.Sprintf("state %+v witn userKey %v has an invalid userEntry %+v", key, userKey, userEntry))
			}
			enc := coders.NewEncoder()
			st.keyCoder.Encode(enc, userKey)
			userKeyBytes := enc.Data()
			if userEntry.cleared {
				c := st.initVals.Clearer([]byte(key.k), []byte(key.w), userKeyBytes)
				c.Write(nil)
			}

			w := st.initVals.Appender([]byte(key.k), []byte(key.w), userKeyBytes)
			valEnc := coders.NewEncoder()
			st.valCoder.Encode(valEnc, userEntry.fresh)
			if _, err := w.Write(valEnc.Data()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Clear empties the entire map state.
func (st *StateMap[K, V]) Clear(ec ElmC) {
	st.cache.Clear(ec.keyBytes, ec.winBytes)
}

// Remove clears the value for the given map key.
func (st *StateMap[K, V]) Remove(ec ElmC, mapKey K) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if !entry.valid {
		entry.fresh = map[K]stateCacheEntry[V]{}
	}
	entry.fresh[mapKey] = stateCacheEntry[V]{valid: true, cleared: true}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

// Set the value for the given map key.
func (st *StateMap[K, V]) Set(ec ElmC, mapKey K, mapVal V) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if !entry.valid {
		entry.fresh = map[K]stateCacheEntry[V]{}
	}
	entry.fresh[mapKey] = stateCacheEntry[V]{fresh: mapVal, valid: true, cleared: true}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

// Keys returns all map keys associated with this element's key and window.
// They can be used to do specific operations against this map and state.
func (st *StateMap[K, V]) Keys(ec ElmC) iter.Seq[K] {
	r := st.initKeys.Reader([]byte(ec.keyBytes), []byte(ec.winBytes))
	return iterClosureWithCoder(st.keyCoder, r)
}

// Values returns an iterator for any value associated with the map key.
func (st *StateMap[K, V]) Values(ec ElmC, mapKey K) iter.Seq[V] {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if entry.valid {
		if uEntry, ok := entry.fresh[mapKey]; ok {
			if uEntry.valid {
				return slices.Values([]V{uEntry.fresh})
			}
			if uEntry.cleared {
				return func(yield func(V) bool) {}
			}
		}
	}
	if entry.cleared {
		return func(yield func(V) bool) {}
	}
	enc := coders.NewEncoder()
	st.keyCoder.Encode(enc, mapKey)
	r := st.initVals.Reader([]byte(ec.keyBytes), []byte(ec.winBytes), enc.Data())
	iter := iterClosureWithCoder(st.valCoder, r)

	var vals []V
	for val := range iter {
		vals = append(vals, val)
	}
	if !entry.valid {
		entry.fresh = map[K]stateCacheEntry[V]{}
		entry.valid = true
	}
	if len(vals) > 0 {
		entry.fresh[mapKey] = stateCacheEntry[V]{
			fresh:  vals[0],
			runner: vals[0],
			valid:  true,
			loaded: true,
		}
	}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
	return slices.Values(vals)
}

// Get returns the value if for the map Key if one is set.
func (st *StateMap[K, V]) Get(ec ElmC, mapKey K) (V, bool) {
	for v := range st.Values(ec, mapKey) {
		return v, true
	}
	var zero V
	return zero, false
}

// All returns an iterator over all the key value pairs in the map.
func (st *StateMap[K, V]) All(ec ElmC) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		entry := st.cache.Get(ec.keyBytes, ec.winBytes)
		seen := map[K]bool{}

		if !entry.cleared {
			for k := range st.Keys(ec) {
				seen[k] = true
				if entry.valid {
					if uEntry, ok := entry.fresh[k]; ok {
						if uEntry.valid {
							if !yield(k, uEntry.fresh) {
								return
							}
							continue
						}
						if uEntry.cleared {
							continue
						}
					}
				}
				val, exists := st.Get(ec, k)
				if exists {
					if !yield(k, val) {
						return
					}
				}
			}
		}

		if entry.valid {
			for k, uEntry := range entry.fresh {
				if seen[k] {
					continue
				}
				if uEntry.valid {
					if !yield(k, uEntry.fresh) {
						return
					}
				}
			}
		}
	}
}

// StateSet represents a de-duplicated set of values
// associated with the embedded DoFn, the element's window, and a key.
//
// Values are deduplicated by their encoded value, but additionally, the set
// values must be Go comparable.
type StateSet[E Keys] struct {
	state
	initKeys stateInit
	initVals stateMapInit

	coder coders.Coder[E]

	cache *stateCache[map[E]stateCacheEntry[struct{}]]
}

var _ stateIface = (*StateSet[int])(nil)

func (st *StateSet[E]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetSetSpec().GetElementCoderId()
	st.coder = coderFromProto[E](coders, coderID)

	valuesPBFn := func(key, win, user []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapUserState_{
				MultimapUserState: &fnpb.StateKey_MultimapUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
					MapKey:      user,
				},
			},
		}
	}
	keysPBFn := func(key, win []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapKeysUserState_{
				MultimapKeysUserState: &fnpb.StateKey_MultimapKeysUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
				},
			},
		}
	}

	st.initKeys = stateInit{
		keyPBFn:       keysPBFn,
		stateInitBase: stb,
	}

	st.initVals = stateMapInit{
		valsPBFn:      valuesPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[map[E]stateCacheEntry[struct{}]]()
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

func (st *StateSet[E]) persist() error {
	for key, entry := range st.cache.Iter() {
		if !entry.valid && !entry.cleared {
			continue
		}

		if entry.cleared {
			c := st.initKeys.Clearer([]byte(key.k), []byte(key.w))
			c.Write(nil)
		}

		if !entry.valid {
			continue
		}

		for userElem, userEntry := range entry.fresh {
			if !userEntry.valid && !userEntry.cleared {
				continue
			}
			enc := coders.NewEncoder()
			st.coder.Encode(enc, userElem)
			userElemBytes := enc.Data()
			if userEntry.cleared {
				c := st.initVals.Clearer([]byte(key.k), []byte(key.w), userElemBytes)
				c.Write(nil)
			}
			if userEntry.valid {
				w := st.initVals.Appender([]byte(key.k), []byte(key.w), userElemBytes)
				if _, err := w.Write(nil); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Clear empties the entire set state.
func (st *StateSet[E]) Clear(ec ElmC) {
	st.cache.Clear(ec.keyBytes, ec.winBytes)
}

// Remove removes the value from the set.
func (st *StateSet[E]) Remove(ec ElmC, val E) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if !entry.valid {
		entry.fresh = map[E]stateCacheEntry[struct{}]{}
	}
	entry.fresh[val] = stateCacheEntry[struct{}]{valid: false, cleared: true}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

// Add adds the value to the set.
func (st *StateSet[E]) Add(ec ElmC, val E) {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if !entry.valid {
		entry.fresh = map[E]stateCacheEntry[struct{}]{}
	}
	entry.fresh[val] = stateCacheEntry[struct{}]{valid: true, cleared: true}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
}

// Contains returns true if the value is present in the set.
func (st *StateSet[E]) Contains(ec ElmC, val E) bool {
	entry := st.cache.Get(ec.keyBytes, ec.winBytes)
	if entry.valid {
		if uEntry, ok := entry.fresh[val]; ok {
			if uEntry.valid {
				return true
			}
			if uEntry.cleared {
				return false
			}
		}
	}
	if entry.cleared {
		return false
	}
	enc := coders.NewEncoder()
	st.coder.Encode(enc, val)
	r := st.initVals.Reader([]byte(ec.keyBytes), []byte(ec.winBytes), enc.Data())
	iter := iterClosureWithCoder(st.coder, r)
	exists := false
	for range iter {
		exists = true
		break
	}
	if !entry.valid {
		entry.fresh = map[E]stateCacheEntry[struct{}]{}
		entry.valid = true
	}
	if exists {
		entry.fresh[val] = stateCacheEntry[struct{}]{
			valid:  true,
			loaded: true,
		}
	}
	st.cache.Put(ec.keyBytes, ec.winBytes, entry)
	return exists
}

// Read returns an iterator over all values in the set.
func (st *StateSet[E]) Read(ec ElmC) iter.Seq[E] {
	return func(yield func(E) bool) {
		entry := st.cache.Get(ec.keyBytes, ec.winBytes)
		seen := map[E]bool{}

		if !entry.cleared {
			r := st.initKeys.Reader([]byte(ec.keyBytes), []byte(ec.winBytes))
			for k := range iterClosureWithCoder(st.coder, r) {
				seen[k] = true
				if entry.valid {
					if uEntry, ok := entry.fresh[k]; ok {
						if uEntry.valid {
							if !yield(k) {
								return
							}
							continue
						}
						if uEntry.cleared {
							continue
						}
					}
				}
				if !yield(k) {
					return
				}
			}
		}

		if entry.valid {
			for k, uEntry := range entry.fresh {
				if seen[k] {
					continue
				}
				if uEntry.valid {
					if !yield(k) {
						return
					}
				}
			}
		}
	}
}

// All returns an iterator over all values in the set.
func (st *StateSet[E]) All(ec ElmC) iter.Seq[E] {
	return st.Read(ec)
}


// StateMultiMap represents a mapping of keys to lists of values.
type StateMultiMap[K Keys, V Element] struct {
	state
	initKeys stateInit
	initVals stateMapInit

	keyCoder coders.Coder[K]
	valCoder coders.Coder[V]

	cache *stateCache[map[K]stateCacheEntry[[]V]]
}

var _ stateIface = (*StateMultiMap[int, int])(nil)

func (st *StateMultiMap[K, V]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	keyCoderID := spec.GetMultimapSpec().GetKeyCoderId()
	st.keyCoder = coderFromProto[K](coders, keyCoderID)

	valCoderID := spec.GetMultimapSpec().GetValueCoderId()
	st.valCoder = coderFromProto[V](coders, valCoderID)

	valuesPBFn := func(key, win, user []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapUserState_{
				MultimapUserState: &fnpb.StateKey_MultimapUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
					MapKey:      user,
				},
			},
		}
	}
	keysPBFn := func(key, win []byte) *fnpb.StateKey {
		return &fnpb.StateKey{
			Type: &fnpb.StateKey_MultimapKeysUserState_{
				MultimapKeysUserState: &fnpb.StateKey_MultimapKeysUserState{
					TransformId: transformID,
					UserStateId: stateID,
					Key:         key,
					Window:      win,
				},
			},
		}
	}

	st.initKeys = stateInit{
		keyPBFn:       keysPBFn,
		stateInitBase: stb,
	}
	st.initVals = stateMapInit{
		valsPBFn:      valuesPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[map[K]stateCacheEntry[[]V]]()
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
	init  stateInit
	coder coders.Coder[E]

	cache *stateCache[[]orderedEntry[E]]
}

type orderedEntry[E Element] struct {
	EventTime time.Time
	Val       E
}

var _ stateIface = (*StateOrderedList[int])(nil)

func (st *StateOrderedList[E]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetOrderedListSpec().GetElementCoderId()
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
	st.init = stateInit{
		keyPBFn:       keyPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[[]orderedEntry[E]]()
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

// AsStateCombining uses a [Combiner] to produce a combining state.
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
	init stateInit

	comb  Combiner[A, I, O, AM]
	coder coders.Coder[A]

	cache *stateCache[A]
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

func (st *StateCombining[A, I, O, AM]) initialize(stb *stateInitBase, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder) {
	coderID := spec.GetCombiningSpec().GetAccumulatorCoderId()
	st.coder = coderFromProto[A](coders, coderID)

	// TODO Extract combiner.

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
	st.init = stateInit{
		keyPBFn:       keyPBFn,
		stateInitBase: stb,
	}
	st.cache = newStateCache[A]()

	panic("unimplemented")
}
