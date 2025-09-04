package beam_test

import (
	"context"
	"testing"

	"lostluck.dev/beam-go"
)

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
