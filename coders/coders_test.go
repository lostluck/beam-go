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

type testBinaryCustom struct {
	text string
}

func (c *testBinaryCustom) MarshalBinary() ([]byte, error) {
	return []byte(c.text), nil
}

func (c *testBinaryCustom) UnmarshalBinary(data []byte) error {
	c.text = string(data)
	return nil
}

type testWindow struct {
	val int
}

func (w testWindow) Encode(enc *Encoder) {
	enc.Int(w.val)
}

func (w testWindow) decode(dec *Decoder) {
	_ = dec.Int()
}

func (w testWindow) String() string {
	return "testWindow"
}

func TestCoders_SpecialTypes(t *testing.T) {
	// BinaryUnmarshaler
	orig := &testBinaryCustom{text: "custom-binary"}
	data, _ := orig.MarshalBinary()
	enc := NewEncoder()
	enc.Bytes(data)
	dec := NewDecoder(enc.Data())
	target := &testBinaryCustom{}
	dec.DecodeBinaryUnmarshaler(target)
	if target.text != "custom-binary" {
		t.Errorf("BinaryUnmarshaler got %v, want custom-binary", target.text)
	}

	// GlobalWindow
	enc.Reset(0)
	enc.GlobalWindow()
	dec = NewDecoder(enc.Data())
	dec.GlobalWindow()
	if !dec.Empty() {
		t.Errorf("expected decoder to be empty after GlobalWindow")
	}

	// IntervalWindow & Nullable
	enc.Reset(0)
	enc.IntervalWindow(time.UnixMilli(1000), time.Second)
	enc.Nullable(true)

	// Pane
	enc.Reset(0)
	enc.Pane(PaneInfo{})
	dec = NewDecoder(enc.Data())
	_ = dec.Pane()

	// WindowedValueHeader decoding
	enc.Reset(0)
	now := time.UnixMilli(987654000)
	enc.Timestamp(now)
	enc.Uint32(1)
	enc.Int(123)
	enc.Pane(PaneInfo{})
	dec = NewDecoder(enc.Data())
	gotTime, gotWins, _ := DecodeWindowedValueHeader[testWindow](dec)
	if gotTime.UnixMilli() != now.UnixMilli() {
		t.Errorf("WindowedValueHeader time = %v, want %v", gotTime, now)
	}
	if len(gotWins) != 1 {
		t.Errorf("gotWins = %+v, want 1 window", gotWins)
	}

	// Byte / StringUtf8
	enc.Reset(0)
	enc.Byte(byte(42))
	enc.StringUtf8("hello utf8")
	dec = NewDecoder(enc.Data())
	if b := dec.Byte(); b != byte(42) {
		t.Errorf("dec.Byte = %v, want 42", b)
	}
	if s := dec.StringUtf8(); s != "hello utf8" {
		t.Errorf("dec.StringUtf8 = %v, want hello utf8", s)
	}
}

func TestCoders_Panics(t *testing.T) {
	// Bool invalid byte panic
	dec := NewDecoder([]byte{2})
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected Bool(2) to panic")
			}
		}()
		_ = dec.Bool()
	}()

	// Read past length panic
	dec2 := NewDecoder([]byte{1, 2})
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected Read(5) on 2-byte slice to panic")
			}
		}()
		_ = dec2.Read(5)
	}()
}

