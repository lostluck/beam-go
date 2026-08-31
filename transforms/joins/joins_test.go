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

package joins_test

import (
	"fmt"
	"strings"
	"testing"

	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/transforms/joins"
	"lostluck.dev/beam-go/transforms/testing/passert"
)

func pipeName(t testing.TB) beam.Options {
	n := strings.ReplaceAll(t.Name(), "/", "_")
	return beam.Name(fmt.Sprintf("%s_%d", n, t.Context().Value("test_id")))
}

type FormatJoinedPairsFn struct {
	Output beam.PCol[string]
}

func (fn *FormatJoinedPairsFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, beam.KV[string, int]]]) error {
	return dfc.Process(func(ec beam.ElmC, elm beam.KV[string, beam.KV[string, int]]) error {
		fn.Output.Emit(ec, fmt.Sprintf("%s: (%s, %d)", elm.Key, elm.Value.Key, elm.Value.Value))
		return nil
	})
}

func TestInnerJoin(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		users := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k2", "Bob"),
			beam.Pair("k3", "Charlie"),
		)
		orders := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k1", 150),
			beam.Pair("k2", 200),
			beam.Pair("k4", 400),
		)

		joined := joins.InnerJoin(s, users, orders)
		formatted := s.ParDo(joined, &FormatJoinedPairsFn{})
		passert.Equals(s, formatted.Output,
			"k1: (Alice, 100)",
			"k1: (Alice, 150)",
			"k2: (Bob, 200)",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

func TestLeftJoin(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		users := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k2", "Bob"),
			beam.Pair("k3", "Charlie"),
		)
		orders := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k2", 200),
			beam.Pair("k4", 400),
		)

		joined := joins.LeftJoin(s, users, orders, -1)
		formatted := s.ParDo(joined, &FormatJoinedPairsFn{})
		passert.Equals(s, formatted.Output,
			"k1: (Alice, 100)",
			"k2: (Bob, 200)",
			"k3: (Charlie, -1)",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

func TestRightJoin(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		users := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k2", "Bob"),
			beam.Pair("k3", "Charlie"),
		)
		orders := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k2", 200),
			beam.Pair("k4", 400),
		)

		joined := joins.RightJoin(s, users, orders, "UNKNOWN")
		formatted := s.ParDo(joined, &FormatJoinedPairsFn{})
		passert.Equals(s, formatted.Output,
			"k1: (Alice, 100)",
			"k2: (Bob, 200)",
			"k4: (UNKNOWN, 400)",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}

func TestFullOuterJoin(t *testing.T) {
	_, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
		users := s.Create(
			beam.Pair("k1", "Alice"),
			beam.Pair("k2", "Bob"),
			beam.Pair("k3", "Charlie"),
		)
		orders := s.Create(
			beam.Pair("k1", 100),
			beam.Pair("k2", 200),
			beam.Pair("k4", 400),
		)

		joined := joins.FullOuterJoin(s, users, orders, "NONE", -1)
		formatted := s.ParDo(joined, &FormatJoinedPairsFn{})
		passert.Equals(s, formatted.Output,
			"k1: (Alice, 100)",
			"k2: (Bob, 200)",
			"k3: (Charlie, -1)",
			"k4: (NONE, 400)",
		)
		return nil
	}, pipeName(t))
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
}
