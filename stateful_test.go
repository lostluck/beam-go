package beam_test

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"

	"lostluck.dev/beam-go"
)

// TODO: Add TestStream, so we can also validate multi-bundle handling.
// TODO: Especially handling clears in multi bundle context.
//     Would a simpler transaction replay approach help?
// TODO: Add sliced state handling, like for outputs.
// TODO: Tests for the remaining state types.
// TODO: Tests for the caching logic as well.
// TODO: Blind writes, and other orderings.
// TODO: Sequence machines for doing state, so we can just have a simple
// handler.

func TestStatefulParDo_Invalid(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			// Test OK.
			return
		}
		t.Error("expected StatefulParDo to panic")
	}()

	_, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, beam.KV[int, int]{1, 2})
		beam.StatefulParDo(s, input, &countFn[beam.KV[int, int]]{})
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	t.Fail() // Unreachable.
}

type StateBagDoFn struct {
	MyBag beam.StateBag[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateBagDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		iter := df.MyBag.Read(ec)
		var sum, count int
		for v := range iter {
			count++
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if count >= 3 {
			df.MyBag.Clear(ec)
		}
		df.MyBag.Append(ec, k.Value)
		return nil
	})
}

func TestStatefulParDo_BagWrites(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 0}, {1, 1}, {1, 3}, {1, 6}, {1, 4}, {2, 0}, {2, 1}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {1, 4}, {1, 5}, {2, 3}}...)
		bagged := beam.StatefulParDo(s, input, &StateBagDoFn{})
		beam.ParDo(s, bagged.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
	if got, want := pr.Counters["sink.Miss"], int64(0); got != want {
		t.Errorf("sink.Miss didn't match bench number: got %v want %v", got, want)
	}
}

type StateBagDoFn_Blind struct {
	MyBag beam.StateBag[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateBagDoFn_Blind) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		df.MyBag.Append(ec, k.Value) // Blind Write.

		iter := df.MyBag.Read(ec)
		var sum, count int
		for v := range iter {
			count++
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if count >= 3 {
			df.MyBag.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_BlindBagWrites(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 1}, {1, 3}, {1, 6}, {1, 4}, {1, 9}, {2, 1}, {2, 4}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {1, 4}, {1, 5}, {2, 3}}...)
		bagged := beam.StatefulParDo(s, input, &StateBagDoFn_Blind{})
		beam.ParDo(s, bagged.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
	if got, want := pr.Counters["sink.Miss"], int64(0); got != want {
		t.Errorf("sink.Miss didn't match bench number: got %v want %v", got, want)
	}
}

type StateValueDoFn struct {
	MyValue beam.StateValue[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateValueDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		val, exists := df.MyValue.Get(ec)
		if !exists && val != 0 {
			return fmt.Errorf("Unset State MyValue has non-zero value")
		}
		sum := val + k.Value
		df.MyValue.Set(ec, sum)

		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MyValue.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_Value(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 1}, {1, 3}, {1, 6}, {1, 10}, {1, 5}, {2, 1}, {2, 4}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {1, 4}, {1, 5}, {2, 3}}...)
		bagged := beam.StatefulParDo(s, input, &StateValueDoFn{})
		beam.ParDo(s, bagged.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
	if got, want := pr.Counters["sink.Miss"], int64(0); got != want {
		t.Errorf("sink.Miss didn't match bench number: got %v want %v", got, want)
	}
}

type StateMapDoFn struct {
	MyMap beam.StateMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMapDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		mapKey := fmt.Sprint(k.Key)
		val, exists := df.MyMap.Get(ec, mapKey)
		if !exists && val != 0 {
			return fmt.Errorf("Unset State MyValue has non-zero value")
		}
		sum := val + k.Value
		df.MyMap.Set(ec, mapKey, sum)

		// Exercise Keys() iterator
		_ = df.MyMap.Keys(ec)

		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MyMap.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_Map(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 1}, {1, 3}, {1, 6}, {1, 10}, {1, 5}, {2, 1}, {2, 4}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {1, 4}, {1, 5}, {2, 3}}...)
		mapped := beam.StatefulParDo(s, input, &StateMapDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
	if got, want := pr.Counters["sink.Miss"], int64(0); got != want {
		t.Errorf("sink.Miss didn't match bench number: got %v want %v", got, want)
	}
}

type StateMapRemoveDoFn struct {
	MyMap beam.StateMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMapRemoveDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		mapKey := fmt.Sprint(k.Key)
		val, _ := df.MyMap.Get(ec, mapKey)
		sum := val + k.Value
		if sum >= 5 {
			df.MyMap.Remove(ec, mapKey)
		} else {
			df.MyMap.Set(ec, mapKey, sum)
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		return nil
	})
}

func TestStatefulParDo_MapRemove(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 2}, {1, 5}, {1, 3}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 2}, {1, 3}, {1, 3}}...)
		mapped := beam.StatefulParDo(s, input, &StateMapRemoveDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateMapAllDoFn struct {
	MyMap beam.StateMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMapAllDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		df.MyMap.Set(ec, fmt.Sprint(k.Value), k.Value)

		var total int
		for _, v := range df.MyMap.All(ec) {
			total += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, total))
		return nil
	})
}

func TestStatefulParDo_MapAll(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 10}, {1, 30}, {2, 5}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 10}, {1, 20}, {2, 5}}...)
		mapped := beam.StatefulParDo(s, input, &StateMapAllDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateSetDoFn struct {
	MySet beam.StateSet[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateSetDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		if df.MySet.Contains(ec, k.Value) {
			df.MySet.Remove(ec, k.Value)
		} else {
			df.MySet.Add(ec, k.Value)
		}

		var sum int
		for v := range df.MySet.Read(ec) {
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 20 {
			df.MySet.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_Set(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 5}, {1, 15}, {1, 5}, {2, 10}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 5}, {1, 10}, {1, 10}, {2, 10}}...)
		setted := beam.StatefulParDo(s, input, &StateSetDoFn{})
		beam.ParDo(s, setted.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateSetClearDoFn struct {
	MySet beam.StateSet[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateSetClearDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		df.MySet.Add(ec, k.Value)
		var sum int
		for v := range df.MySet.All(ec) {
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MySet.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_SetClear(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 5}, {1, 15}, {1, 20}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 5}, {1, 10}, {1, 20}}...)
		setted := beam.StatefulParDo(s, input, &StateSetClearDoFn{})
		beam.ParDo(s, setted.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type MultiStateDoFn struct {
	Count  beam.StateValue[int]
	Values beam.StateBag[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *MultiStateDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		cnt, _ := df.Count.Get(ec)
		df.Count.Set(ec, cnt+1)
		df.Values.Append(ec, k.Value)

		var total int
		for v := range df.Values.Read(ec) {
			total += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, total))
		return nil
	})
}

func TestStatefulParDo_MultiState(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 10}, {1, 30}, {2, 5}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 10}, {1, 20}, {2, 5}}...)
		st := beam.StatefulParDo(s, input, &MultiStateDoFn{})
		beam.ParDo(s, st.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type kvSimpleFac struct{}

func (kvSimpleFac) Setup() error { return nil }
func (kvSimpleFac) InitialSplit(_ beam.KV[int, int], r beam.OffsetRange) iter.Seq2[beam.OffsetRange, float64] {
	return func(yield func(beam.OffsetRange, float64) bool) {
		yield(r, 1.0)
	}
}
func (kvSimpleFac) Produce(_ beam.KV[int, int]) beam.OffsetRange {
	return beam.OffsetRange{Min: 0, Max: 10}
}

type SDFStatefulFn struct {
	SDF beam.BoundedSDF[kvSimpleFac, beam.KV[int, int], *beam.ORTracker, beam.OffsetRange, int64, bool]
	Val beam.StateValue[int]
}

func (df *SDFStatefulFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return nil
}

func TestStatefulParDo_SDFPanic(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			return
		}
		t.Error("expected StatefulParDo to panic when passed an SDF")
	}()

	_, _ = beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, beam.KV[int, int]{1, 2})
		beam.StatefulParDo(s, input, &SDFStatefulFn{})
		return nil
	}, pipeName(t))
}

type StateValueCacheDoFn struct {
	MyVal beam.StateValue[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateValueCacheDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		v1, ok1 := df.MyVal.Get(ec)
		if ok1 || v1 != 0 {
			return fmt.Errorf("unexpected initial value: got (%v, %v), want (0, false)", v1, ok1)
		}

		df.MyVal.Set(ec, 10)
		v2, ok2 := df.MyVal.Get(ec)
		if !ok2 || v2 != 10 {
			return fmt.Errorf("unexpected value after Set(10): got (%v, %v), want (10, true)", v2, ok2)
		}

		df.MyVal.Clear(ec)
		v3, ok3 := df.MyVal.Get(ec)
		if ok3 || v3 != 0 {
			return fmt.Errorf("unexpected value after Clear: got (%v, %v), want (0, false)", v3, ok3)
		}

		df.MyVal.Set(ec, 42)
		v4, ok4 := df.MyVal.Get(ec)
		if !ok4 || v4 != 42 {
			return fmt.Errorf("unexpected value after Set(42): got (%v, %v), want (42, true)", v4, ok4)
		}

		df.Output.Emit(ec, beam.Pair(k.Key, v4))
		return nil
	})
}

func TestStatefulParDo_ValueCacheInvariants(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 42}, {2, 42}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {2, 1}}...)
		st := beam.StatefulParDo(s, input, &StateValueCacheDoFn{})
		beam.ParDo(s, st.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateMultiMapDoFn struct {
	MyMultiMap beam.StateMultiMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMultiMapDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		mapKey := fmt.Sprint(k.Key)
		df.MyMultiMap.Append(ec, mapKey, k.Value)

		var sum int
		for v := range df.MyMultiMap.Get(ec, mapKey) {
			sum += v
		}

		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MyMultiMap.Remove(ec, mapKey)
		}
		return nil
	})
}

func TestStatefulParDo_MultiMap(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 1}, {1, 3}, {1, 6}, {1, 10}, {1, 5}, {2, 1}, {2, 4}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {1, 4}, {1, 5}, {2, 3}}...)
		mapped := beam.StatefulParDo(s, input, &StateMultiMapDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
	if got, want := pr.Counters["sink.Miss"], int64(0); got != want {
		t.Errorf("sink.Miss didn't match bench number: got %v want %v", got, want)
	}
}

type StateMultiMapAllDoFn struct {
	MyMultiMap beam.StateMultiMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMultiMapAllDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		mapKey := fmt.Sprint(k.Value)
		df.MyMultiMap.Append(ec, mapKey, k.Value)

		// Exercise Keys() iterator
		keysCount := 0
		for range df.MyMultiMap.Keys(ec) {
			keysCount++
		}

		var total int
		for _, v := range df.MyMultiMap.All(ec) {
			total += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, total+keysCount))
		return nil
	})
}

func TestStatefulParDo_MultiMapAllAndKeys(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 11}, {1, 32}, {2, 6}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 10}, {1, 20}, {2, 5}}...)
		mapped := beam.StatefulParDo(s, input, &StateMultiMapAllDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateMultiMapClearDoFn struct {
	MyMultiMap beam.StateMultiMap[string, int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateMultiMapClearDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		mapKey := fmt.Sprint(k.Key)
		df.MyMultiMap.Append(ec, mapKey, k.Value)

		var sum int
		for v := range df.MyMultiMap.Get(ec, mapKey) {
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MyMultiMap.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_MultiMapClear(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 5}, {1, 15}, {1, 20}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 5}, {1, 10}, {1, 20}}...)
		mapped := beam.StatefulParDo(s, input, &StateMultiMapClearDoFn{})
		beam.ParDo(s, mapped.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateOrderedListDoFn struct {
	MyList beam.StateOrderedList[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateOrderedListDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		t := time.UnixMilli(int64(k.Value) * 100)
		df.MyList.AppendWithTimestamp(ec, t, k.Value)

		var values []int
		for v := range df.MyList.Read(ec) {
			values = append(values, v)
		}

		var allValues []int
		for v := range df.MyList.All(ec) {
			allValues = append(allValues, v)
		}
		if !slices.Equal(values, allValues) {
			return fmt.Errorf("All mismatch: got %v, want %v", allValues, values)
		}

		var entriesCount int
		for ts, v := range df.MyList.ReadEntries(ec) {
			if ts.UnixMilli() != int64(v)*100 {
				return fmt.Errorf("unexpected entry timestamp: got %v, want %v", ts.UnixMilli(), int64(v)*100)
			}
			entriesCount++
		}
		if entriesCount != len(values) {
			return fmt.Errorf("ReadEntries count mismatch: got %v, want %v", entriesCount, len(values))
		}

		sum := 0
		for _, v := range values {
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		return nil
	})
}

func TestStatefulParDo_OrderedList(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 3}, {1, 4}, {1, 6}, {2, 5}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 3}, {1, 1}, {1, 2}, {2, 5}}...)
		ordered := beam.StatefulParDo(s, input, &StateOrderedListDoFn{})
		beam.ParDo(s, ordered.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateOrderedListRangeDoFn struct {
	MyList beam.StateOrderedList[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateOrderedListRangeDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		t := time.UnixMilli(int64(k.Value) * 100)
		df.MyList.AppendWithTimestamp(ec, t, k.Value)

		minT := time.UnixMilli(200)
		maxT := time.UnixMilli(400)

		var rangeSum int
		for v := range df.MyList.ReadRange(ec, minT, maxT) {
			rangeSum += v
		}

		for ts := range df.MyList.ReadRangeEntries(ec, minT, maxT) {
			if ts.Before(minT) || !ts.Before(maxT) {
				return fmt.Errorf("ReadRangeEntries yielded out of range timestamp: %v", ts)
			}
		}

		df.Output.Emit(ec, beam.Pair(k.Key, rangeSum))
		return nil
	})
}

func TestStatefulParDo_OrderedListRange(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 0}, {1, 2}, {1, 5}, {1, 5}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 1}, {1, 2}, {1, 3}, {1, 4}}...)
		ranged := beam.StatefulParDo(s, input, &StateOrderedListRangeDoFn{})
		beam.ParDo(s, ranged.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}

type StateOrderedListClearDoFn struct {
	MyList beam.StateOrderedList[int]

	Output beam.PCol[beam.KV[int, int]]
}

func (df *StateOrderedListClearDoFn) ProcessBundle(dfc *beam.DFC[beam.KV[int, int]]) error {
	return dfc.Process(func(ec beam.ElmC, k beam.KV[int, int]) error {
		df.MyList.Append(ec, k.Value)

		var sum int
		for v := range df.MyList.Read(ec) {
			sum += v
		}
		df.Output.Emit(ec, beam.Pair(k.Key, sum))
		if sum >= 10 {
			df.MyList.Clear(ec)
		}
		return nil
	})
}

func TestStatefulParDo_OrderedListClear(t *testing.T) {
	expected := []beam.KV[int, int]{{1, 5}, {1, 15}, {1, 20}}

	pr, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		input := beam.Create(s, []beam.KV[int, int]{{1, 5}, {1, 10}, {1, 20}}...)
		cleared := beam.StatefulParDo(s, input, &StateOrderedListClearDoFn{})
		beam.ParDo(s, cleared.Output, &countFn[beam.KV[int, int]]{Countable: expected}, beam.Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("LaunchAndWait produced an error: %v", err)
	}
	if got, want := pr.Counters["sink.Hit"], int64(len(expected)); got != want {
		t.Errorf("sink.Hit didn't match bench number: got %v want %v", got, want)
	}
}


