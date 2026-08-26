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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lostluck.dev/beam-go"
)

type collectCountsFn struct {
	Count beam.CounterInt64
}

func (fn *collectCountsFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, int]]) error {
	return dfc.Process(func(ec beam.ElmC, kv beam.KV[string, int]) error {
		fn.Count.Inc(dfc, int64(kv.Value))
		return nil
	})
}

func TestCountWords(t *testing.T) {
	ctx := t.Context()
	p, err := beam.LaunchAndWait(ctx, func(s *beam.Scope) error {
		lines := beam.Create(s,
			"To be, or not to be: that is the question:",
			"",
			"Whether 'tis nobler in the mind to suffer",
		)
		wordCounts := CountWords(s, lines, 5)
		beam.ParDo(s, wordCounts, &collectCountsFn{}, beam.Name("collector"))
		return nil
	})
	if err != nil {
		t.Fatalf("CountWords pipeline failed: %v", err)
	}

	// Verify metrics
	if p.Counters["extract.EmptyLines"] != 1 {
		t.Errorf("EmptyLines = %v, want 1", p.Counters["extract.EmptyLines"])
	}
	if p.Counters["extract.SmallWords"] <= 0 {
		t.Errorf("expected SmallWords > 0, got %v", p.Counters["extract.SmallWords"])
	}
	if p.Distributions["extract.LineLen"].Count != 3 {
		t.Errorf("LineLen distribution count = %v, want 3", p.Distributions["extract.LineLen"].Count)
	}
}

func TestSum(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		tests := []struct {
			name string
			a, b int
			want int
		}{
			{name: "positive", a: 3, b: 4, want: 7},
			{name: "negative", a: -5, b: 2, want: -3},
			{name: "zero", a: 0, b: 0, want: 0},
		}
		sInt := sum[int]{}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if got := sInt.MergeAccumulators(tc.a, tc.b); got != tc.want {
					t.Errorf("MergeAccumulators(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
				}
			})
		}
	})

	t.Run("float64", func(t *testing.T) {
		tests := []struct {
			name string
			a, b float64
			want float64
		}{
			{name: "decimals", a: 1.5, b: 2.5, want: 4.0},
			{name: "negative", a: -1.2, b: 0.2, want: -1.0},
		}
		sFloat := sum[float64]{}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if got := sFloat.MergeAccumulators(tc.a, tc.b); got != tc.want {
					t.Errorf("MergeAccumulators(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
				}
			})
		}
	})
}

func TestWordcountPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "input.txt")
	outPath := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outPath, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(inPath, []byte("hello world hello"), 0644); err != nil {
		t.Fatal(err)
	}

	pipelineFn := func(s *beam.Scope) error {
		lines := beam.Create(s, "hello world hello", "foo bar baz")
		wordcount := CountWords(s, lines, 5)
		formatted := beam.Map(s, wordcount, func(count beam.KV[string, int]) string {
			return strings.ToUpper(count.Key)
		})
		beam.ParDo(s, formatted, &verifyStringFn{})
		return nil
	}

	_, err := beam.LaunchAndWait(t.Context(), pipelineFn)
	if err != nil {
		t.Fatalf("wordcount pipeline failed: %v", err)
	}

	// Verify wordcountPipeline registers cleanly
	cfg := beam.New()
	cfg.Load("wordcount", wordcountPipeline())
}

type verifyStringFn struct {
	Count beam.CounterInt64
}

func (fn *verifyStringFn) ProcessBundle(dfc *beam.DFC[string]) error {
	return dfc.Process(func(ec beam.ElmC, s string) error {
		fn.Count.Inc(dfc, 1)
		return nil
	})
}
