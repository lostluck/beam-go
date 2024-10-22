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

package coders

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func roundTripMakeCoder[T any](v T) struct {
	val   any
	coder func(v any) any
} {
	return struct {
		val   any
		coder func(v any) any
	}{
		val: v,
		coder: func(v any) any {
			c := MakeCoder[T]()
			e := NewEncoder()
			c.Encode(e, v.(T))
			d := NewDecoder(e.Data())
			return c.Decode(d)
		},
	}
}

func TestMakeCoder(t *testing.T) {
	tests := []struct {
		val   any
		coder func(any) any
	}{
		roundTripMakeCoder(bool(false)),
		roundTripMakeCoder(bool(true)),
		roundTripMakeCoder(int8(3)),
		roundTripMakeCoder(int16(4)),
		roundTripMakeCoder(int32(5)),
		roundTripMakeCoder(int64(6)),
		roundTripMakeCoder(uint8(7)),
		roundTripMakeCoder(uint16(8)),
		roundTripMakeCoder(uint32(9)),
		roundTripMakeCoder(uint64(10)),
		roundTripMakeCoder(uint(11)),
		roundTripMakeCoder(int(12)),
		roundTripMakeCoder(float32(13)),
		roundTripMakeCoder(float64(14)),
		roundTripMakeCoder(complex64(15 + 15i)),
		roundTripMakeCoder(complex128(16 + 16i)),
		roundTripMakeCoder("squeamish ossifrage"),
		roundTripMakeCoder([]byte{8, 3, 7, 4, 6, 0, 9}),

		// TODO: Arrays
		// TODO: Slices
		// TODO: Maps

		// Row coder tests
		roundTripMakeCoder(struct{ T time.Time }{T: time.Now()}),
		roundTripMakeCoder(struct{ S string }{S: "pajamas"}),
		roundTripMakeCoder(struct{ I int }{I: -42}),
		//	roundTripMakeCoder(&struct{ Any int }{Any: 0xDEADBEEF}), // Pointer test

		roundTripMakeCoder(struct{ s int }{}), // TODO: Forbid empty types?
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.val).Name(), func(t *testing.T) {
			got, want := test.coder(test.val), test.val
			var opts []cmp.Option
			switch reflect.TypeOf(test.val).Kind() {
			case reflect.Struct:
				opts = append(opts, cmp.AllowUnexported(test.val))
			}
			if d := cmp.Diff(want, got, opts...); d != "" {
				t.Errorf("MakeCoder[%T]() round trip failed. got %v want %v, diff (-want, +got):\n%v", test.val, got, want, d)
			}
		})
	}
}
