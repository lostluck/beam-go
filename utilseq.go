package beam

import (
	"io"
	"iter"
	"time"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
)

// utilseq.go is where various iter.Seq utility functions go, since they
// are used in various places in Beam.

// Concatenate iters together.
// Ignores nil iterators.
func concat[E any](iters ...iter.Seq[E]) iter.Seq[E] {
	return func(yield func(E) bool) {
		for _, iter := range iters {
			if iter == nil {
				continue
			}
			for v := range iter {
				if !yield(v) {
					return
				}
			}
		}
	}
}

func iterClosure[E Element](r harness.NextBuffer) iter.Seq[E] {
	c := MakeCoder[E]()
	return iterClosureWithCoder(c, r)
}

func iterClosureWithCoder[E Element](c coders.Coder[E], r harness.NextBuffer) iter.Seq[E] {
	return func(perElm func(elm E) bool) {
		defer func() {
			_ = r.Close()
		}()
		for {
			buf, err := r.NextBuf()
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(err)
			}
			dec := coders.NewDecoder(buf)
			for !dec.Empty() {
				if !perElm(c.Decode(dec)) {
					return
				}
			}
		}
	}
}

func iterClosureWithTimestampCoder[E Element](c coders.Coder[E], r harness.NextBuffer) iter.Seq2[time.Time, E] {
	return func(perElm func(ts time.Time, elm E) bool) {
		defer func() {
			_ = r.Close()
		}()
		for {
			buf, err := r.NextBuf()
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(err)
			}
			dec := coders.NewDecoder(buf)
			for !dec.Empty() {
				ts := dec.Timestamp()
				val := c.Decode(dec)
				if !perElm(ts, val) {
					return
				}
			}
		}
	}
}
