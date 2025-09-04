package beam

import (
	"context"
	"fmt"
	"reflect"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
	"lostluck.dev/beam-go/internal/harness"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// This is where we're having the graph construction logic for Stateful DoFns.

// StatefulParDo allows adding a StatefulDoFn to the pipeline.
// StatefulDoFns are DoFns that use state or timers. They must have a KV as
// as an element. State and timers are scoped to the key, and the window of
// the element.
//
// A stateful DoFn inherently observes the window of the element.
//
// In execution, all elements with a given key and window will be executed
// sequentially, and use the same state.
//
// This function panics if provided a DoFn without state or timers, as it is a
// programming error.
func StatefulParDo[DF Transform[KV[K, V]], K Keys, V Element](s *Scope, input PCol[KV[K, V]], dofn DF, opts ...Options) DF {
	fields := extractStateful(dofn)
	if len(fields) == 0 {
		panic(fmt.Sprintf("Non-stateful DoFn %T passed to StatefulParDo. Must have a State or Timer typed field", dofn))
	}

	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	ins, outs, sides, extras := s.g.deferDoFn(dofn, input.globalIndex, edgeID)

	if extras.sdf != nil {
		panic(fmt.Sprintf("%T passed to StatefulParDo. Stateful DoFns not be an SDF. Please split into two", dofn))
	}

	s.g.edges = append(s.g.edges, &edgeDoFn[KV[K, V]]{index: edgeID, dofn: &hiddenKeyedStateful[DF, K, V]{DoFn: dofn}, ins: ins, outs: outs, sides: sides, parallelIn: input.globalIndex, opts: opt, states: extras.states})

	return dofn
}

var (
	statefaceRT = reflect.TypeOf((*stateIface)(nil)).Elem()
	timerfaceRT = reflect.TypeOf((*timerIface)(nil)).Elem()
)

// IsStateful returns a list of the stateful fields in the DoFn.
func extractStateful[E Element](dofn Transform[E]) []string {
	rt := reflect.TypeOf(dofn).Elem()
	var ret []string
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)

		ptrf := reflect.PointerTo(f.Type)

		if ptrf.Implements(statefaceRT) || ptrf.Implements(timerfaceRT) {
			ret = append(ret, f.Name)
		}
	}
	return ret
}

// edgeKeyedDoFn is for handling stateful DoFns which require elements to be
// KV pairs.
type edgeKeyedDoFn[K Keys, V Element] struct {
	*edgeDoFn[KV[K, V]]

	// Static state for DoFn reconstruction.
	states                map[string]*pipepb.StateSpec
	timers                map[string]*pipepb.TimerFamilySpec
	onWindowExpiryTimerID string
	coders                map[string]*pipepb.Coder
	coderID               string
}

// Initialize the Beam state of the DoFn's fields.
func (e *edgeKeyedDoFn[K, V]) initializeDoFn(ctx context.Context, dataCon harness.DataContext, stateURL string) any {
	// TODO use onWindowExpiryTimerID
	return e.dofn.(stateful).initialize(ctx, dataCon, stateURL, e.transformID(), e.states, e.timers, e.coders, e.coderID)
}

type keyedEdge interface {
	initializeDoFn(ctx context.Context, dataCon harness.DataContext, stateURL string) any
}

type stateful interface {
	keyed(e multiEdge, wrap *dofnWrap, coders map[string]*pipepb.Coder, coderID string) multiEdge
	initialize(ctx context.Context, dataCon harness.DataContext, url string, transformID string, states map[string]*pipepb.StateSpec, timers map[string]*pipepb.TimerFamilySpec, coders map[string]*pipepb.Coder, coderID string) any
	getUserTransform() any
}

type hiddenKeyedStateful[T Transform[KV[K, V]], K Keys, V Element] struct {
	DoFn T

	OnBundleFinish

	keyCoder coders.Coder[K]
}

func (fn *hiddenKeyedStateful[T, K, V]) keyed(e multiEdge, wrap *dofnWrap, coders map[string]*pipepb.Coder, coderID string) multiEdge {
	return &edgeKeyedDoFn[K, V]{
		edgeDoFn:              e.(*edgeDoFn[KV[K, V]]),
		states:                wrap.states,
		timers:                wrap.timers,
		onWindowExpiryTimerID: wrap.onWindowExpiryTimerID,
		coders:                coders,
		coderID:               coderID,
	}
}

// initialize implements stateful.
func (fn *hiddenKeyedStateful[T, K, V]) initialize(ctx context.Context, dataCon harness.DataContext, url string, transformID string, states map[string]*pipepb.StateSpec, timers map[string]*pipepb.TimerFamilySpec, coders map[string]*pipepb.Coder, coderID string) any {
	rv := reflect.ValueOf(fn.DoFn).Elem()

	if len(states) == 0 {
		panic("no states")
	}
	// TODO make handling LP unwraps a general function?
	kvCoder := coders[coderID]
	if kvCoder.GetSpec().GetUrn() == "beam:coder:length_prefix:v1" {
		kvCoder = coders[kvCoder.GetComponentCoderIds()[0]]
	}
	keyCoderID := kvCoder.GetComponentCoderIds()[0]
	fn.keyCoder = coderFromProto[K](coders, keyCoderID)

	// Initialize states
	for stateID, spec := range states {
		// TODO: Also support fixed array of some states.
		fv := rv.FieldByName(stateID)
		if !fv.IsValid() {
			panic(fmt.Sprintf("unknown state field with ID %v, for transform type %T", stateID, fn.DoFn))
		}
		// TODO: COLLECT STATES HERE, so we can call them OnBundleFinish, when we move to transaction tracking
		if st, ok := fv.Addr().Interface().(stateIface); ok {
			st.initialize(ctx, dataCon, url, stateID, transformID, spec, coders)
		} else {

			panic(fmt.Sprintf("unnown state field with ID %v, doesn't implement stateful for field type %v", stateID, fv.Type()))
		}
	}
	return fn.DoFn
}

func (fn *hiddenKeyedStateful[T, K, V]) getUserTransform() any {
	return fn.DoFn
}

func (fn *hiddenKeyedStateful[T, K, V]) ProcessBundle(dfc *DFC[KV[K, V]]) error {
	if err := fn.DoFn.ProcessBundle(dfc); err != nil {
		return err
	}
	// Now that dfc is primed with the user's per element, we need to wrap it
	// so that we can pass the encoded key context down to the user side.
	userPerElm := dfc.perElm

	// TODO, replace with getCoderFromProto
	memoKeys := map[K][]byte{}
	dfc.perElm = func(ec ElmC, e KV[K, V]) error {
		kb, exists := memoKeys[e.Key]
		if !exists {
			enc := coders.NewEncoder()
			fn.keyCoder.Encode(enc, e.Key)
			kb = enc.Data()
			memoKeys[e.Key] = kb
		}
		ec.keyBytes = kb
		return userPerElm(ec, e)
	}
	return nil
}

var _ stateful = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
var _ Transform[KV[int, int]] = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
