// Package synthetic produces elements and load.
// Typically used for load testing, and scale testing.
package synthetic

import (
	"iter"
	"math"
	"math/rand/v2"
	"time"

	"lostluck.dev/beam-go"
)

// What needs to happen is this:
// First I need to get SDF hooked up, which means
// having a real separation harness test going.
// And from there write a real synthetic generator (useful enough for streaming team).
//
// I should should have the closure convenience handler.
// The construction time pipeline option resolver, and the worker
// side option initializer.

// syntheticStep is a DoFn which can be controlled with prespecified parameters.
type syntheticStep[E beam.Element] struct {
	PerElementDelay, PerBundleDelay time.Duration
	OutputRecordsPerInputRecord     uint
	OutputFilterRatio               float64

	beam.OnBundleFinish
	Output beam.PCol[E]
}

func (fn *syntheticStep[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	startTime := time.Now()

	fn.OnBundleFinish.Do(dfc, func() error {
		// The target is for the enclosing stage to take as close to as possible
		// the given number of seconds, so we only sleep enough to make up for
		// overheads not incurred elsewhere.
		toSleep := fn.PerBundleDelay - (time.Since(startTime))
		time.Sleep(toSleep)
		return nil
	})

	return dfc.Process(func(ec beam.ElmC, e E) error {
		time.Sleep(fn.PerElementDelay)
		filterElement := false
		if fn.OutputFilterRatio > 0 {
			if rand.Float64() < fn.OutputFilterRatio {
				filterElement = true
			}
		}
		if filterElement {
			return nil
		}
		for range fn.OutputRecordsPerInputRecord {
			fn.Output.Emit(ec, e)
		}
		return nil
	})
}

// syntheticSDFStep is a Splittable DoFn which can be controlled with prespecified parameters.
type syntheticSDFStep[RF beam.RestrictionFactory[E, beam.OffsetRange, int64], T beam.Tracker[beam.OffsetRange, int64], E beam.Element] struct {
	PerElementDelay, PerBundleDelay time.Duration
	OutputRecordsPerInputRecord     uint // TODO Needs to be folded into the factory.
	OutputFilterRatio               float64

	// SDF Specific configuration.
	InitialSplittingNumBundles   uint
	InitialSplittingUnevenChunks bool

	MakeTracker func(beam.OffsetRange) T

	beam.OnBundleFinish
	Output beam.PCol[E]
	beam.BoundedSDF[RF, E, T, beam.OffsetRange, int64, bool]
}

func (fn *syntheticSDFStep[RF, T, E]) ProcessBundle(dfc *beam.DFC[E]) error {
	startTime := time.Now()

	fn.OnBundleFinish.Do(dfc, func() error {
		// The target is for the enclosing stage to take as close to as possible
		// the given number of seconds, so we only sleep enough to make up for
		// overheads not incurred elsewhere.
		toSleep := fn.PerBundleDelay - (time.Since(startTime))
		time.Sleep(toSleep)
		return nil
	})

	return fn.BoundedSDF.Process(dfc,
		fn.MakeTracker,
		func(ec beam.ElmC, e E, or beam.OffsetRange, tc beam.TryClaim[int64]) error {
			filterElement := false
			if fn.OutputFilterRatio > 0 {
				if rand.Float64() < fn.OutputFilterRatio {
					filterElement = true
				}
			}

			return tc(func(p int64) (int64, error) {
				time.Sleep(fn.PerElementDelay)
				if !filterElement {
					fn.Output.Emit(ec, e)
				}
				return p + 1, nil
			})
		})
}

type SourceConfig struct {
	NumRecords                    int
	KeySize, ValueSize            int
	InitialSplitNumBundles        int
	InitialSplitDesiredBundleSize int
	SleepPerInputRecord           time.Duration
	InitialSplit                  string
}

type syntheticSourceRestrictionFactory struct {
}

func (syntheticSourceRestrictionFactory) Setup() error {
	return nil
}

func (syntheticSourceRestrictionFactory) InitialSplit(cfg SourceConfig, rest beam.OffsetRange) iter.Seq2[beam.OffsetRange, float64] {
	elmSize := cfg.KeySize + cfg.ValueSize
	switch cfg.InitialSplit {
	case "zipf":
		panic("unimplemented")
	default:
		elmsPerBundle := max(1, cfg.NumRecords/cfg.InitialSplitNumBundles)
		if cfg.InitialSplitNumBundles < 1 {
			elmsPerBundle = int(math.Ceil(float64(cfg.InitialSplitDesiredBundleSize) / float64(elmSize)))
		}
		return func(yield func(beam.OffsetRange, float64) bool) {
			for start := rest.Min; start < rest.Max; start += int64(elmsPerBundle) {
				stop := min(start+int64(elmsPerBundle), rest.Max)
				out := beam.OffsetRange{Min: start, Max: stop}
				yield(out, float64(out.Max-out.Min))
			}
		}
	}
}

func (syntheticSourceRestrictionFactory) Produce(cfg SourceConfig) beam.OffsetRange {
	return beam.OffsetRange{Min: 0, Max: int64(cfg.NumRecords)}
}

type syntheticSDFAsSource struct {
	beam.OnBundleFinish
	Output beam.PCol[beam.KV[[]byte, []byte]]
	beam.BoundedSDF[syntheticSourceRestrictionFactory, SourceConfig, *beam.ORTracker, beam.OffsetRange, int64, bool]
}
