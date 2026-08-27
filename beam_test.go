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
	"flag"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"golang.org/x/exp/constraints"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
)

// TODO sort out running tests in non-loopback mode.

type SourceFn struct {
	Count  int
	Output PCol[int]
}

func (fn *SourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	// Do some startbundle work.
	processed := 0
	return dfc.Process(func(ec ElmC, _ []byte) error {
		for i := 0; i < fn.Count; i++ {
			processed++
			fn.Output.Emit(ec, i)
		}
		return nil
	})
}

type DiscardFn[E Element] struct {
	OnBundleFinish

	Processed, Finished CounterInt64
}

func (fn *DiscardFn[E]) ProcessBundle(dfc *DFC[E]) error {
	if err := dfc.Process(func(ec ElmC, elm E) error {
		fn.Processed.Inc(dfc, 1)
		return nil
	}); err != nil {
		return err
	}
	fn.Do(dfc, func() error {
		fn.Finished.Inc(dfc, 1)
		return nil
	})
	return nil
}

type IdenFn[E Element] struct {
	Output PCol[E]

	BundleStarts CounterInt64
}

func (fn *IdenFn[E]) ProcessBundle(dfc *DFC[E]) error {
	fn.BundleStarts.Inc(dfc, 1)
	return dfc.Process(func(ec ElmC, elm E) error {
		fn.Output.Emit(ec, elm)
		return nil
	})
}

func pipeName(tb testing.TB) beamopts.Options {
	return Name(tb.Name())
}

func TestSimple(t *testing.T) {
	_, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: 10})
		s.ParDo(src.Output, &DiscardFn[int]{})
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
}

func TestAutomaticDiscard(t *testing.T) {
	_, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		s.ParDo(imp, &SourceFn{Count: 10})
		// drop the output.
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
}

func TestSimpleNamed(t *testing.T) {
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: 10})
		s.ParDo(src.Output, &DiscardFn[int]{}, Name("pants"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	t.Log(pr.Counters)
	if got, want := int(pr.Counters["pants.Processed"]), 10; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: lostluck.dev/beam-go
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/var_dofns_0-16         	70822042	        16.65 ns/op	 480.54 MB/s	        16.65 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_1-16         	35603048	        33.70 ns/op	 474.83 MB/s	        33.70 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_2-16         	24342855	        48.95 ns/op	 490.25 MB/s	        24.48 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_3-16         	17800094	        66.13 ns/op	 483.91 MB/s	        22.04 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_5-16         	12088483	        99.11 ns/op	 484.32 MB/s	        19.82 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_10-16        	 6605112	       181.5 ns/op	 484.75 MB/s	        18.15 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_100-16       	  582006	      2030 ns/op	 398.00 MB/s	        20.30 ns/elm	       0 B/op	       0 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(8 * int64(numDoFns+1))

			pr, err := LaunchAndWait(b.Context(), func(s *Scope) error {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: b.N})
				iden := src.Output
				for range numDoFns {
					iden = s.ParDo(iden, &IdenFn[int]{}).Output
				}
				s.ParDo(iden, &DiscardFn[int]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Errorf("Run error: %v", err)
			}
			if got, want := int(pr.Counters["sink.Processed"]), b.N; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
			if got, want := int(pr.Counters["sink.Finished"]), 1; got != want {
				b.Fatalf("finished didn't match bundle counter: got %v want %v", got, want)
			}
			d := b.Elapsed()
			div := numDoFns
			if div == 0 {
				div = 1
			}
			div = div * b.N
			b.ReportMetric(float64(d)/float64(div), "ns/elm")
		}
	}
	for _, numDoFns := range []int{0, 0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("dofns=%d", numDoFns), makeBench(numDoFns))
	}
}

type ModPartition[V constraints.Integer] struct {
	Outputs []PCol[V] // The count needs to be properly serialized, ultimately.
}

func (fn *ModPartition[V]) ProcessBundle(dfc *DFC[V]) error {
	mod := V(len(fn.Outputs))
	return dfc.Process(func(ec ElmC, elm V) error {
		rem := elm % mod
		fn.Outputs[rem].Emit(ec, elm)
		return nil
	})
}

type WideNarrow struct {
	Wide int

	In PCol[int]
}

var _ Composite[struct{ Out PCol[int] }] = ((*WideNarrow)(nil))

func (src *WideNarrow) Expand(s *Scope) (out struct{ Out PCol[int] }) {
	partition := s.ParDo(src.In, &ModPartition[int]{Outputs: make([]PCol[int], src.Wide)})
	out.Out = s.Flatten(partition.Outputs...)
	return out
}

func TestPartitionFlatten(t *testing.T) {
	count, mod := 10, 2
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: count})
		exp := s.Expand("WideNarrow", &WideNarrow{Wide: mod, In: src.Output})
		s.ParDo(exp.Out, &DiscardFn[int]{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), count; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Finished"]), 1; got != want {
		t.Fatalf("finished didn't match bundle countr: got %v want %v", got, want)
	}
}

// BenchmarkPartitionPipe benchmarks dispatch across arbitrary partioning, and a flatten.
//
// goos: linux
// goarch: amd64
// pkg: lostluck.dev/beam-go
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPartitionPipe/num_partitions_1-16         	26054823	        45.68 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_2-16         	25842020	        45.76 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_3-16         	26205663	        45.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_5-16         	26325379	        45.63 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_10-16        	26314922	        45.64 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_100-16       	26035390	        45.79 ns/op	       0 B/op	       0 allocs/op
func BenchmarkPartitionPipe(b *testing.B) {
	makeBench := func(numPartitions int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()

			pr, err := LaunchAndWait(b.Context(), func(s *Scope) error {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: b.N})
				exp := s.Expand("WideNarrow", &WideNarrow{Wide: numPartitions, In: src.Output})
				s.ParDo(exp.Out, &DiscardFn[int]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			if got, want := int(pr.Counters["sink.Processed"]), b.N; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		}
	}
	for _, numDoFns := range []int{1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("num_partitions=%d", numDoFns), makeBench(numDoFns))
	}
}

type KeyMod[V constraints.Integer] struct {
	Mod V

	Output PCol[KV[V, V]]
}

func (fn *KeyMod[V]) ProcessBundle(dfc *DFC[V]) error {
	return dfc.Process(func(ec ElmC, elm V) error {
		mod := elm % fn.Mod
		fn.Output.Emit(ec, KV[V, V]{
			Key:   V(mod),
			Value: elm,
		})
		return nil
	})
}

type SumByKey[K Keys, V constraints.Integer | constraints.Float] struct {
	Output PCol[KV[K, V]]
}

func (fn *SumByKey[K, V]) ProcessBundle(dfc *DFC[KV[K, Iter[V]]]) error {
	return dfc.Process(func(ec ElmC, elm KV[K, Iter[V]]) error {
		var sum V
		elm.Value.All()(func(elm V) bool {
			sum += elm
			return true
		})
		fn.Output.Emit(ec, KV[K, V]{Key: elm.Key, Value: sum})
		return nil
	})
}

type GroupKeyModSum[V constraints.Integer] struct {
	Mod V

	Output PCol[KV[V, V]]

	OnBundleFinish
}

var (
	MaxET time.Time = time.UnixMilli(math.MaxInt64 / 1000)
	EOGW            = MaxET.Add(-time.Hour * 24)
)

func (fn *GroupKeyModSum[V]) ProcessBundle(dfc *DFC[V]) error {
	grouped := map[V]V{}
	if err := dfc.Process(func(ec ElmC, elm V) error {
		mod := elm % fn.Mod
		v := grouped[mod]
		v += elm
		grouped[mod] = v
		return nil
	}); err != nil {
		return err
	}

	fn.Do(dfc, func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull from the window that's been closed.
		for k, v := range grouped {
			fn.Output.Emit(ec, KV[V, V]{Key: k, Value: v})
		}
		return nil
	})
	return nil
}

func TestGBKSum(t *testing.T) {
	mod := 3
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: 10})
		keyed := s.ParDo(src.Output, &KeyMod[int]{Mod: mod})
		grouped := s.GBK(keyed.Output)
		sums := s.ParDo(grouped, &SumByKey[int, int]{})
		s.ParDo(sums.Output, &DiscardFn[KV[int, int]]{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), mod; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

func BenchmarkGBKSum_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			discard := &DiscardFn[KV[int, int]]{}
			pr, err := LaunchAndWait(b.Context(), func(s *Scope) error {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: b.N})
				keyed := s.ParDo(src.Output, &KeyMod[int]{Mod: mod})
				grouped := s.GBK(keyed.Output)
				sums := s.ParDo(grouped, &SumByKey[int, int]{})
				s.ParDo(sums.Output, discard, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			want := min(b.N, mod)
			if got, want := int(pr.Counters["sink.Processed"]), want; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		})
	}
}

func BenchmarkGBKSum_Lifted_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			pr, err := LaunchAndWait(b.Context(), func(s *Scope) error {
				imp := s.Impulse()
				src := s.ParDo(imp, &SourceFn{Count: b.N})
				keyed := s.ParDo(src.Output, &GroupKeyModSum[int]{Mod: mod})
				s.ParDo(keyed.Output, &DiscardFn[KV[int, int]]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			want := min(b.N, mod)
			if got, want := int(pr.Counters["sink.Processed"]), want; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		})
	}
}

func TestTwoSubGraphs(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp1, imp2 := s.Impulse(), s.Impulse()
		src1, src2 := s.ParDo(imp1, &SourceFn{Count: count + 1}), s.ParDo(imp2, &SourceFn{Count: count + 2})
		s.ParDo(src1.Output, &DiscardFn[int]{}, Name("sink1"))
		s.ParDo(src2.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count+1; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count+2; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}

func TestMultiplexImpulse(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse() // As a Runner transform, impulses don't multiplex.
		src1, src2 := s.ParDo(imp, &SourceFn{Count: count + 1}), s.ParDo(imp, &SourceFn{Count: count + 2})
		s.ParDo(src1.Output, &DiscardFn[int]{}, Name("sink1"))
		s.ParDo(src2.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count+1; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count+2; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}

func TestMultiplex(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &SourceFn{Count: count})
		s.ParDo(src.Output, &DiscardFn[int]{}, Name("sink1"))
		s.ParDo(src.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}

func TestPair_And_ElmC(t *testing.T) {
	p := Pair("k", 123)
	if p.Key != "k" || p.Value != 123 {
		t.Errorf("Pair = %+v", p)
	}

	now := time.Now()
	ec := ElmC{eventTime: now}
	if ec.EventTime() != now {
		t.Errorf("EventTime() = %v, want %v", ec.EventTime(), now)
	}
}

func TestIter_All_And_MetaType(t *testing.T) {
	items := []int{10, 20, 30}
	idx := 0
	it := Iter[int]{
		source: func() (int, bool) {
			if idx >= len(items) {
				return 0, false
			}
			v := items[idx]
			idx++
			return v, true
		},
	}

	var got []int
	for v := range it.All() {
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != 10 || got[1] != 20 || got[2] != 30 {
		t.Errorf("Iter.All() = %v, want [10, 20, 30]", got)
	}

	// Test early break
	idx = 0
	var early []int
	for v := range it.All() {
		early = append(early, v)
		if len(early) == 1 {
			break
		}
	}
	if len(early) != 1 {
		t.Errorf("Iter.All() early break failed: %v", early)
	}

	if !isMetaType(Iter[int]{}) {
		t.Errorf("Iter[int] should be meta type")
	}
	if isMetaType(123) {
		t.Errorf("int should not be meta type")
	}
}

func TestScope_String(t *testing.T) {
	root := &Scope{name: "root"}
	child := &Scope{name: "child", parent: root}

	tests := []struct {
		name  string
		scope *Scope
		want  string
	}{
		{name: "nil_scope", scope: nil, want: ""},
		{name: "root_scope", scope: root, want: "/root"},
		{name: "child_scope", scope: child, want: "/root/child"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.scope.String(); got != tc.want {
				t.Errorf("Scope(%s).String() = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestConfiguration(t *testing.T) {
	cfg := New()
	if cfg == nil || cfg.pipelines == nil {
		t.Fatalf("New() configuration invalid")
	}

	// Flags
	cfg.Flags(flag.NewFlagSet("test", flag.ContinueOnError))
	cfg.FromCommandLine()

	// Load valid
	cfg.Load("p1", func(s *Scope) error { return nil })

	// Load nil expand
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected Load(nil) to panic")
		}
	}()
	cfg.Load("pNil", nil)
}

func TestConfiguration_DuplicateLoad(t *testing.T) {
	cfg := New()
	cfg.Load("dup", func(s *Scope) error { return nil })
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected duplicate Load to panic")
		}
	}()
	cfg.Load("dup", func(s *Scope) error { return nil })
}

func TestLauncher_Errors(t *testing.T) {
	var emptyLauncher Launcher
	_, err := emptyLauncher.Run(t.Context(), "pid")
	if err == nil {
		t.Errorf("expected error for empty launcher")
	}

	cfg := New()
	cfg.Load("valid", func(s *Scope) error {
		return fmt.Errorf("construction failure")
	})
	launcher := cfg.Ready(t.Context())

	// Unregistered PID
	_, err = launcher.Run(t.Context(), "unregistered")
	if err == nil {
		t.Errorf("expected error for unregistered PID")
	}

	// Construction error
	_, err = launcher.Run(t.Context(), "valid")
	if err == nil {
		t.Errorf("expected error for failing pipeline expansion")
	}
}

func TestExtractEnv(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name    string
		flags   *envFlags
		wantUrn string
		wantErr bool
	}{
		{
			name:    "loopback",
			flags:   &envFlags{EnvironmentType: "LOOPBACK"},
			wantUrn: "beam:env:external:v1",
			wantErr: false,
		},
		{
			name:    "docker_default",
			flags:   &envFlags{EnvironmentType: "DOCKER"},
			wantUrn: "beam:env:docker:v1",
			wantErr: false,
		},
		{
			name:    "docker_custom",
			flags:   &envFlags{EnvironmentType: "DOCKER", EnvironmentConfig: "custom-image:v1"},
			wantUrn: "beam:env:docker:v1",
			wantErr: false,
		},
		{
			name: "process_valid",
			flags: &envFlags{
				EnvironmentType:   "PROCESS",
				EnvironmentConfig: `{"os": "linux", "arch": "amd64", "command": "echo"}`,
			},
			wantUrn: "beam:env:process:v1",
			wantErr: false,
		},
		{
			name: "process_invalid_json",
			flags: &envFlags{
				EnvironmentType:   "PROCESS",
				EnvironmentConfig: `invalid-json`,
			},
			wantErr: true,
		},
		{
			name:    "unknown_type",
			flags:   &envFlags{EnvironmentType: "UNKNOWN"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := extractEnv(ctx, tc.flags, nil, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("extractEnv(%+v) error = %v, wantErr = %v", tc.flags, err, tc.wantErr)
			}
			if !tc.wantErr && env.Urn != tc.wantUrn {
				t.Errorf("extractEnv(%+v).Urn = %q, want %q", tc.flags, env.Urn, tc.wantUrn)
			}
		})
	}
}

func TestOffsetRange_And_ORTracker(t *testing.T) {
	r := OffsetRange{Min: 10, Max: 50}
	if r.Start() != 10 || r.End() != 50 {
		t.Errorf("Start/End failed: %+v", r)
	}
	if !r.Bounded() {
		t.Errorf("expected Bounded() = true")
	}
	unbounded := OffsetRange{Min: 0, Max: math.MaxInt64}
	if unbounded.Bounded() {
		t.Errorf("expected Bounded() = false for MaxInt64")
	}

	tracker := &ORTracker{Rest: r}
	if tracker.Size(r) != 40.0 {
		t.Errorf("Size = %v, want 40", tracker.Size(r))
	}
	if tracker.GetRestriction() != r {
		t.Errorf("GetRestriction = %+v, want %+v", tracker.GetRestriction(), r)
	}

	// TryClaim below Min
	if tracker.TryClaim(5) {
		t.Errorf("TryClaim(5) should return false for Min=10")
	}
	if tracker.GetError() == nil {
		t.Errorf("expected error after claiming below Min")
	}

	// TryClaim after stopped
	if tracker.TryClaim(15) {
		t.Errorf("TryClaim after stopped should return false")
	}

	// Fresh tracker
	tracker = &ORTracker{Rest: r}
	if !tracker.TryClaim(10) {
		t.Errorf("TryClaim(10) failed")
	}
	// TryClaim non-monotonic (<= claimed)
	if tracker.TryClaim(10) {
		t.Errorf("TryClaim(10) again should fail")
	}

	// Another fresh tracker
	tracker = &ORTracker{Rest: r}
	if !tracker.TryClaim(15) {
		t.Errorf("TryClaim(15) failed")
	}
	if tracker.IsDone() {
		t.Errorf("tracker should not be done yet")
	}

	done, remaining := tracker.GetProgress()
	if done != 6 || remaining != 34 {
		t.Errorf("GetProgress = (%v, %v), want (6, 34)", done, remaining)
	}

	// TrySplit negative fraction (clamped to 0)
	prim, res, err := tracker.TrySplit(-0.5)
	if err != nil {
		t.Errorf("TrySplit(-0.5) error: %v", err)
	}
	if prim.Max != 16 || res.Min != 16 {
		t.Errorf("TrySplit(-0.5) got prim=%+v, res=%+v", prim, res)
	}

	// TrySplit fraction > 1 (clamped to 1)
	tracker2 := &ORTracker{Rest: OffsetRange{Min: 0, Max: 100}}
	tracker2.TryClaim(0)
	prim2, res2, err := tracker2.TrySplit(1.5)
	if err != nil || res2 != (OffsetRange{}) {
		t.Errorf("TrySplit(1.5) got prim=%+v, res=%+v, err=%v", prim2, res2, err)
	}

	// TryClaim >= Max
	tracker3 := &ORTracker{Rest: OffsetRange{Min: 0, Max: 10}}
	if tracker3.TryClaim(10) {
		t.Errorf("TryClaim(10) for Max=10 should return false")
	}
	if !tracker3.IsDone() {
		t.Errorf("tracker3 should be done after claiming >= Max")
	}
}

type mockNextBuf struct {
	bufs [][]byte
}

func (m *mockNextBuf) NextBuf() ([]byte, error) {
	if len(m.bufs) == 0 {
		return nil, io.EOF
	}
	b := m.bufs[0]
	m.bufs = m.bufs[1:]
	return b, nil
}

func (m *mockNextBuf) Reset() error { return nil }
func (m *mockNextBuf) Close() error { return nil }

func TestUtilSeq_Concat_And_Closures(t *testing.T) {
	it1 := func(yield func(int) bool) {
		if yield(1) {
			yield(2)
		}
	}
	it2 := func(yield func(int) bool) {
		yield(3)
	}

	cat := concat(it1, nil, it2)
	var got []int
	for v := range cat {
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("concat got %v, want [1, 2, 3]", got)
	}

	// Early break
	var early []int
	for v := range cat {
		early = append(early, v)
		if len(early) == 2 {
			break
		}
	}
	if len(early) != 2 {
		t.Errorf("concat early break got %v", early)
	}

	// iterClosure with coder
	enc := coders.NewEncoder()
	coders.MakeCoder[int]().Encode(enc, 42)
	coders.MakeCoder[int]().Encode(enc, 99)

	mb := &mockNextBuf{bufs: [][]byte{enc.Data()}}
	var closureVals []int
	for v := range iterClosure[int](mb) {
		closureVals = append(closureVals, v)
	}
	if len(closureVals) != 2 || closureVals[0] != 42 || closureVals[1] != 99 {
		t.Errorf("iterClosure got %v, want [42, 99]", closureVals)
	}

	// iterClosureWithTimestampCoder
	now := time.UnixMilli(12345000)
	enc2 := coders.NewEncoder()
	enc2.Timestamp(now)
	coders.MakeCoder[string]().Encode(enc2, "timed")

	mb2 := &mockNextBuf{bufs: [][]byte{enc2.Data()}}
	var tsVals []string
	for ts, v := range iterClosureWithTimestampCoder(coders.MakeCoder[string](), mb2) {
		if ts.UnixMilli() != now.UnixMilli() {
			t.Errorf("ts = %v, want %v", ts, now)
		}
		tsVals = append(tsVals, v)
	}
	if len(tsVals) != 1 || tsVals[0] != "timed" {
		t.Errorf("iterClosureWithTimestampCoder got %v", tsVals)
	}
}

func TestEdgeFlatten_Methods(t *testing.T) {
	ef := &edgeFlatten[int]{
		index:     edgeIndex(1),
		transform: "t_flatten",
		ins:       []nodeIndex{2, 3},
		output:    nodeIndex(4),
	}

	if ef.protoID() != "t_flatten" {
		t.Errorf("protoID = %v, want t_flatten", ef.protoID())
	}
	if ef.edgeID() != edgeIndex(1) {
		t.Errorf("edgeID = %v, want 1", ef.edgeID())
	}

	ins := ef.inputs()
	if len(ins) != 2 || ins["i0"] != nodeIndex(2) || ins["i1"] != nodeIndex(3) {
		t.Errorf("inputs = %+v", ins)
	}

	outs := ef.outputs()
	if len(outs) != 1 || outs["Output"] != nodeIndex(4) {
		t.Errorf("outputs = %+v", outs)
	}

	spec, envID, name := ef.toProtoParts(translateParams{})
	if spec.Urn != "beam:transform:flatten:v1" || envID != "" || name != "Flatten" {
		t.Errorf("toProtoParts = (%v, %v, %v)", spec, envID, name)
	}

	tr, inst, procs, first := ef.flatten()
	if tr != "t_flatten" || inst == nil || len(procs) != 1 || !first {
		t.Errorf("flatten() first call failed: (%v, %v, %v, %v)", tr, inst, procs, first)
	}

	// Second call should return first = false
	_, _, _, first2 := ef.flatten()
	if first2 {
		t.Errorf("second call to flatten() should have first = false")
	}
}

type sliceSourceFn struct {
	Output PCol[[]string]
}

func (fn *sliceSourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		fn.Output.Emit(ec, []string{"alpha", "beta", "gamma"})
		fn.Output.Emit(ec, []string{"one", "two"})
		return nil
	})
}

type sliceTransformFn struct {
	Output PCol[[]int]
}

func (fn *sliceTransformFn) ProcessBundle(dfc *DFC[[]string]) error {
	return dfc.Process(func(ec ElmC, elm []string) error {
		lengths := make([]int, len(elm))
		for i, s := range elm {
			lengths[i] = len(s)
		}
		fn.Output.Emit(ec, lengths)
		return nil
	})
}

func TestSlicePipeline(t *testing.T) {
	_, err := LaunchAndWait(t.Context(), func(s *Scope) error {
		imp := s.Impulse()
		src := s.ParDo(imp, &sliceSourceFn{})
		lengths := s.ParDo(src.Output, &sliceTransformFn{})
		s.ParDo(lengths.Output, &DiscardFn[[]int]{})
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("Slice pipeline failed: %v", err)
	}
}
