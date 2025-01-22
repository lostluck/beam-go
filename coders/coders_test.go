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
			data := Encode(c, v.(T))
			return Decode(c, data)
		},
	}
}

type manualCoder[T any] struct {
	encode func(enc *Encoder, v T)
	decode func(dec *Decoder) T
}

var _ Coder[int] = (*manualCoder[int])(nil)

func (c *manualCoder[T]) Encode(enc *Encoder, v T) {
	c.encode(enc, v)
}

func (c *manualCoder[T]) Decode(dec *Decoder) T {
	return c.decode(dec)
}

func makeManualCoder[T any](encode func(enc *Encoder, v T), decode func(dec *Decoder) T) Coder[T] {
	return &manualCoder[T]{encode: encode, decode: decode}
}

func manualRoundTripCoder[T any](v T, encode func(enc *Encoder, v T), decode func(dec *Decoder) T) struct {
	val   any
	coder func(v any) any
} {
	return struct {
		val   any
		coder func(v any) any
	}{
		val: v,
		coder: func(v any) any {
			c := makeManualCoder(encode, decode)
			data := Encode(c, v.(T))
			return Decode(c, data)
		},
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
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

		manualRoundTripCoder(int64(19), (*Encoder).Int64, (*Decoder).Int64),
		manualRoundTripCoder(int32(20), (*Encoder).Int32, (*Decoder).Int32),
		manualRoundTripCoder(int16(21), (*Encoder).Int16, (*Decoder).Int16),
		manualRoundTripCoder(int8(22), (*Encoder).Int8, (*Decoder).Int8),
		manualRoundTripCoder(int(23), (*Encoder).Int, (*Decoder).Int),
		manualRoundTripCoder(uint64(24), (*Encoder).Uint64, (*Decoder).Uint64),
		manualRoundTripCoder(uint32(25), (*Encoder).Uint32, (*Decoder).Uint32),
		manualRoundTripCoder(uint16(26), (*Encoder).Uint16, (*Decoder).Uint16),
		manualRoundTripCoder(uint8(27), (*Encoder).Uint8, (*Decoder).Uint8),
		manualRoundTripCoder(uint(28), (*Encoder).Uint, (*Decoder).Uint),
		manualRoundTripCoder(rune('B'), (*Encoder).Rune, (*Decoder).Rune),

		manualRoundTripCoder(must(time.Parse("2006-01-02", "2024-01-21")), (*Encoder).Timestamp, (*Decoder).Timestamp),
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
