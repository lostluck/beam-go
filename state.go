package beam

import (
	"context"
	"fmt"
	"io"
	"iter"
	"reflect"

	"github.com/go-json-experiment/json"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// State in Beam is associated with a window, and a key, and a DoFn.

// state is a zero sized mixin to be embedded into valid state types as fields.
type state struct{ beamMixin }

func (state) isState() {}

type stateIface interface {
	isState()
	toProtoParts(params translateParams) *pipepb.StateSpec
	initialize(ctx context.Context, dataCon harness.DataContext, url, stateID, transformID string, spec *pipepb.StateSpec, coders map[string]*pipepb.Coder)
}

const (
	urnBagUserState      = "beam:user_state:bag:v1"
	urnMultiMapUserState = "beam:user_state:multimap:v1"
)

// StateBag represents an unordered collection of state associated with the
// embedded DoFn, the element's window, and a key.
type StateBag[E Element] struct {
	state

	initBagReader   func(key, win []byte) harness.NextBuffer
	initBagAppender func(key, win []byte) io.Writer
	initBagClearer  func(key, win []byte) io.Writer
	coder           coders.Coder[E]
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

	// TODO: Track state transactions instead for consolidating on finishing bundle.
}

func (st *StateBag[E]) Append(ec ElmC, val E) {
	if st == nil {
		panic("StateBag somehow nil")
	}
	if st.initBagAppender == nil {
		panic("StateBag.initBagAppender somehow nil")
	}
	winData := coders.NewEncoder()
	win := ec.windows[0]
	win.Encode(winData)
	w := st.initBagAppender(ec.keyBytes, winData.Data())

	vData := coders.NewEncoder()
	st.coder.Encode(vData, val)
	if _, err := w.Write(vData.Data()); err != nil {
		panic(fmt.Sprintf("error on StateBag.Append: %v", err))
	}
}

func (st *StateBag[E]) Clear(ec ElmC) {
	winData := coders.NewEncoder()
	win := ec.windows[0]
	win.Encode(winData)
	c := st.initBagClearer(ec.keyBytes, winData.Data())
	if _, err := c.Write(nil); err != nil {
		panic(fmt.Sprintf("error on StateBag.Clear: %v", err))
	}
}

func (st *StateBag[E]) Read(ec ElmC) iter.Seq[E] {
	winData := coders.NewEncoder()
	win := ec.windows[0]
	win.Encode(winData)
	r := st.initBagReader(ec.keyBytes, winData.Data())
	return iterClosureWithCoder(st.coder, r)
}

// StateValue represents a single value of state associated with the
// embedded DoFn, the element's window, and a key.
type StateValue[E Element] struct {
	state
	coder coders.Coder[E]
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
	panic("unimplemented")
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
