package beam

import (
	"context"
	"fmt"
	"reflect"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
	"lostluck.dev/beam-go/internal/harness"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
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
func (s *Scope) StatefulParDo[DF Transform[KV[K, V]], K Keys, V Element](input PCol[KV[K, V]], dofn DF, opts ...Options) DF {
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

	s.g.edges = append(s.g.edges, &edgeDoFn[KV[K, V]]{
		index:                 edgeID,
		dofn:                  &hiddenKeyedStateful[DF, K, V]{DoFn: dofn},
		ins:                   ins,
		outs:                  outs,
		sides:                 sides,
		parallelIn:            input.globalIndex,
		opts:                  opt,
		states:                extras.states,
		timers:                extras.timers,
		onWindowExpiryTimerID: extras.onWindowExpiryTimerID,
	})

	return dofn
}

var (
	statefaceRT    = reflect.TypeFor[stateIface]()
	timerfaceRT    = reflect.TypeFor[timerIface]()
	windowExpiryRT = reflect.TypeFor[windowExpiryIface]()
)

// IsStateful returns a list of the stateful fields in the DoFn.
func extractStateful[E Element](dofn Transform[E]) []string {
	rt := reflect.TypeOf(dofn).Elem()
	var ret []string
	for f := range rt.Fields() {
		ptrf := reflect.PointerTo(f.Type)
		if ptrf.Implements(statefaceRT) || ptrf.Implements(timerfaceRT) || ptrf.Implements(windowExpiryRT) {
			ret = append(ret, f.Name)
		}
	}
	return ret
}

type timerInitBase struct {
	ctx         context.Context
	dataCon     harness.DataContext
	transformID string
}

type timerExecutor interface {
	executeTimer(familyID string, timerBytes []byte) error
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
	return e.dofn.(stateful).initialize(ctx, dataCon, stateURL, e.transformID(), e.states, e.timers, e.coders, e.coderID)
}

func (e *edgeKeyedDoFn[K, V]) hasTimers() bool {
	return len(e.timers) > 0 || e.onWindowExpiryTimerID != ""
}

type keyedEdge interface {
	initializeDoFn(ctx context.Context, dataCon harness.DataContext, stateURL string) any
	hasTimers() bool
}

type stateful interface {
	keyed(e multiEdge, wrap *dofnWrap, coders map[string]*pipepb.Coder, coderID string) multiEdge
	initialize(ctx context.Context, dataCon harness.DataContext, url string, transformID string, states map[string]*pipepb.StateSpec, timers map[string]*pipepb.TimerFamilySpec, coders map[string]*pipepb.Coder, coderID string) any
	getUserTransform() any
}

type hiddenKeyedStateful[T Transform[KV[K, V]], K Keys, V Element] struct {
	DoFn T

	OnBundleFinish

	keyCoder              coders.Coder[K]
	windowCoder           windowCoder
	stateInterfaces       []stateIface
	timerInterfaces       []timerIface
	onWindowExpiryTimerID string

	timerCallbacks map[string]func(ec ElmC, key any, tag string) error
	downstream     []processor
}

func (fn *hiddenKeyedStateful[T, K, V]) registerTimerCallback(familyID string, cb func(ec ElmC, key any, tag string) error) {
	if fn.timerCallbacks == nil {
		fn.timerCallbacks = map[string]func(ec ElmC, key any, tag string) error{}
	}
	fn.timerCallbacks[familyID] = cb
}

func (fn *hiddenKeyedStateful[T, K, V]) executeTimer(familyID string, timerBytes []byte) error {
	dec := coders.NewDecoder(timerBytes)
	for !dec.Empty() {
		key := fn.keyCoder.Decode(dec)
		tag, numWindows, clearBit := dec.TimerHeader()
		ws := make([]window.BoundedWindow, numWindows)
		wCoder := fn.windowCoder
		if wCoder == nil {
			wCoder = globalWindowCoderWrapper{}
		}
		for i := range ws {
			ws[i] = wCoder.Decode(dec)
		}
		if clearBit {
			continue
		}
		fireTime, _, pane := dec.TimerDetails()

		encKey := coders.NewEncoder()
		fn.keyCoder.Encode(encKey, key)
		kb := string(encKey.Data())

		var win window.BoundedWindow = window.GlobalWindow{}
		if len(ws) > 0 {
			win = ws[0]
		}
		encWin := coders.NewEncoder()
		switch w := win.(type) {
		case window.GlobalWindow:
			encWin.GlobalWindow()
		case window.IntervalWindow:
			encWin.IntervalWindow(w.End, w.Duration())
		default:
			encWin.GlobalWindow()
		}
		wb := string(encWin.Data())

		ec := ElmC{
			eventTime:    fireTime,
			windows:      ws,
			window:       win,
			pane:         pane,
			pcollections: fn.downstream,
			keyBytes:     kb,
			winBytes:     wb,
		}

		if cb, ok := fn.timerCallbacks[familyID]; ok {
			if err := cb(ec, key, tag); err != nil {
				return err
			}
		}
	}
	return nil
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

	if len(states) == 0 && len(timers) == 0 && fn.onWindowExpiryTimerID == "" {
		panic("no states or timers")
	}
	kvCoder := coders[coderID]
	if kvCoder.GetSpec().GetUrn() == "beam:coder:length_prefix:v1" {
		kvCoder = coders[kvCoder.GetComponentCoderIds()[0]]
	}
	keyCoderID := kvCoder.GetComponentCoderIds()[0]
	fn.keyCoder = coderFromProto[K](coders, keyCoderID)

	fn.stateInterfaces = make([]stateIface, 0, len(states))
	stb := &stateInitBase{
		ctx:     ctx,
		dataCon: dataCon,
		url:     url,
	}
	for stateID, spec := range states {
		fv := rv.FieldByName(stateID)
		if !fv.IsValid() {
			panic(fmt.Sprintf("unknown state field with ID %v, for transform type %T", stateID, fn.DoFn))
		}
		if st, ok := fv.Addr().Interface().(stateIface); ok {
			st.initialize(stb, stateID, transformID, spec, coders)
			fn.stateInterfaces = append(fn.stateInterfaces, st)
		} else {
			panic(fmt.Sprintf("unknown state field with ID %v, doesn't implement stateIface for field type %v", stateID, fv.Type()))
		}
	}

	fn.timerInterfaces = make([]timerIface, 0, len(timers))
	tmb := &timerInitBase{
		ctx:         ctx,
		dataCon:     dataCon,
		transformID: transformID,
	}
	for timerID, spec := range timers {
		fv := rv.FieldByName(timerID)
		if !fv.IsValid() {
			panic(fmt.Sprintf("unknown timer field with ID %v, for transform type %T", timerID, fn.DoFn))
		}
		if tm, ok := fv.Addr().Interface().(timerIface); ok {
			tm.initialize(tmb, timerID, transformID, spec, coders)
			fn.timerInterfaces = append(fn.timerInterfaces, tm)

			if fn.windowCoder == nil {
				tCoder := coders[spec.GetTimerFamilyCoderId()]
				if tCoder != nil && len(tCoder.GetComponentCoderIds()) > 1 {
					fn.windowCoder = windowCoderFromProto(coders, tCoder.GetComponentCoderIds()[1])
				}
			}
		} else if we, ok := fv.Addr().Interface().(windowExpiryIface); ok {
			we.initialize(tmb, timerID, transformID, spec, coders)
			if fn.windowCoder == nil {
				tCoder := coders[spec.GetTimerFamilyCoderId()]
				if tCoder != nil && len(tCoder.GetComponentCoderIds()) > 1 {
					fn.windowCoder = windowCoderFromProto(coders, tCoder.GetComponentCoderIds()[1])
				}
			}
		} else {
			panic(fmt.Sprintf("unknown timer field with ID %v, doesn't implement timerIface for field type %v", timerID, fv.Type()))
		}
	}
	if fn.windowCoder == nil {
		fn.windowCoder = globalWindowCoderWrapper{}
	}
	return fn.DoFn
}

func (fn *hiddenKeyedStateful[T, K, V]) getUserTransform() any {
	return fn.DoFn
}

func (fn *hiddenKeyedStateful[T, K, V]) ProcessBundle(dfc *DFC[KV[K, V]]) error {
	fn.downstream = dfc.downstream
	if err := fn.DoFn.ProcessBundle(dfc); err != nil {
		return err
	}
	userPerElm := dfc.perElm
	memoKeys := map[K]string{}
	memoWins := map[window.BoundedWindow]string{}

	if userPerElm != nil {
		dfc.perElm = func(ec ElmC, e KV[K, V]) error {
			kb, exists := memoKeys[e.Key]
			if !exists {
				enc := coders.NewEncoder()
				fn.keyCoder.Encode(enc, e.Key)
				kb = string(enc.Data())
				memoKeys[e.Key] = kb
			}

			win := ec.window
			if win == nil {
				if len(ec.windows) > 0 {
					win = ec.windows[0]
				} else {
					win = window.GlobalWindow{}
				}
			}
			wb, exists := memoWins[win]
			if !exists {
				enc := coders.NewEncoder()
				switch w := win.(type) {
				case window.GlobalWindow:
					enc.GlobalWindow()
				case window.IntervalWindow:
					enc.IntervalWindow(w.End, w.Duration())
				default:
					enc.GlobalWindow()
				}
				wb = string(enc.Data())
				memoWins[win] = wb
			}

			ec.keyBytes = kb
			ec.winBytes = wb
			return userPerElm(ec, e)
		}
	}

	fn.Do(dfc, func() error {
		for _, st := range fn.stateInterfaces {
			if err := st.persist(); err != nil {
				return err
			}
		}
		for _, tm := range fn.timerInterfaces {
			if err := tm.writeTimers(dfc.ctx); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

var _ stateful = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
var _ Transform[KV[int, int]] = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
var _ timerExecutor = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
var _ timerRegistrar = (*hiddenKeyedStateful[Transform[KV[int, int]], int, int])(nil)
