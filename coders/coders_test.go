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
	"bytes"
	"fmt"
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

		// Slices
		roundTripMakeCoder([]int{1, 2, 3, -4, 5}),
		roundTripMakeCoder([]int8{1, 2, -3}),
		roundTripMakeCoder([]int16{10, 20, -30}),
		roundTripMakeCoder([]int32{100, 200, -300}),
		roundTripMakeCoder([]int64{1000, 2000, -3000}),
		roundTripMakeCoder([]uint{11, 22, 33}),
		roundTripMakeCoder([]uint16{111, 222}),
		roundTripMakeCoder([]uint32{1111, 2222}),
		roundTripMakeCoder([]uint64{11111, 22222}),
		roundTripMakeCoder([]bool{true, false, true}),
		roundTripMakeCoder([]float32{1.5, 2.5, -3.5}),
		roundTripMakeCoder([]float64{1.234, 5.678, -9.1011}),
		roundTripMakeCoder([]complex64{1 + 2i, 3 + 4i}),
		roundTripMakeCoder([]complex128{5 + 6i, 7 + 8i}),
		roundTripMakeCoder([]string{"hello", "world", "beam", "go"}),
		roundTripMakeCoder([][]byte{[]byte("foo"), []byte("bar")}),
		roundTripMakeCoder([][]int{{1, 2}, {3, 4, 5}, {}}),
		roundTripMakeCoder([]time.Time{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)}),
		roundTripMakeCoder([]struct{ Name string; Val int }{{Name: "a", Val: 1}, {Name: "b", Val: 2}}),
		roundTripMakeCoder(struct{ List []string; Tags []int }{List: []string{"a", "b"}, Tags: []int{10, 20}}),
		roundTripMakeCoder([]string{}),

		// TODO: Arrays
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
		name := reflect.TypeOf(test.val).String()
		t.Run(name, func(t *testing.T) {
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

func (w *testWindow) decode(dec *Decoder) {
	w.val = dec.Int()
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
	dec = NewDecoder(enc.Data())
	end, dur := dec.IntervalWindow()
	if end.UnixMilli() != 1000 || dur != time.Second {
		t.Errorf("got interval window (%v, %v), want (1000ms, 1s)", end, dur)
	}

	enc.Reset(0)
	enc.Nullable(true)

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

	// Slice coder negative length panic
	enc := NewEncoder()
	enc.Int32(-1)
	sc := MakeCoder[[]int]()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected Decode on negative slice length to panic")
			}
		}()
		dec := NewDecoder(enc.Data())
		_ = sc.Decode(dec)
	}()
}

func TestMakeSliceCoder_NilSlice(t *testing.T) {
	c := MakeCoder[[]string]()
	var nilSlice []string
	data := Encode(c, nilSlice)
	got := Decode(c, data)
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}

func TestMakeSliceCoder_Explicit(t *testing.T) {
	// Explicit elemCoder
	intCoder := MakeCoder[int]()
	sc := MakeSliceCoder[int](intCoder)
	in := []int{10, 20, 30, 40}
	data := Encode(sc, in)
	got := Decode(sc, data)
	if d := cmp.Diff(in, got); d != "" {
		t.Errorf("MakeSliceCoder explicit roundtrip diff (-want, +got):\n%v", d)
	}

	// Implicit nil elemCoder
	scNil := MakeSliceCoder[string](nil)
	inStr := []string{"foo", "bar", "baz"}
	dataStr := Encode(scNil, inStr)
	gotStr := Decode(scNil, dataStr)
	if d := cmp.Diff(inStr, gotStr); d != "" {
		t.Errorf("MakeSliceCoder nil elemCoder roundtrip diff (-want, +got):\n%v", d)
	}
}

func BenchmarkSliceCoder(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		// []int
		intData := make([]int, size)
		for i := range intData {
			intData[i] = i * 100
		}
		genIntCoder := MakeSliceCoder[int](nil)
		refIntCoder := makeSliceCoder[[]int](reflect.TypeFor[[]int]()).(Coder[[]int])
		encIntGen := Encode(genIntCoder, intData)
		encIntRef := Encode(refIntCoder, intData)

		b.Run(fmt.Sprintf("int/size_%d/generic_encode", size), func(b *testing.B) {
			b.ReportAllocs()
			enc := NewEncoder()
			for b.Loop() {
				enc.Reset(0)
				genIntCoder.Encode(enc, intData)
			}
		})
		b.Run(fmt.Sprintf("int/size_%d/reflective_encode", size), func(b *testing.B) {
			b.ReportAllocs()
			enc := NewEncoder()
			for b.Loop() {
				enc.Reset(0)
				refIntCoder.Encode(enc, intData)
			}
		})
		b.Run(fmt.Sprintf("int/size_%d/generic_decode", size), func(b *testing.B) {
			b.ReportAllocs()
			dec := NewDecoder(encIntGen)
			for b.Loop() {
				dec.data = encIntGen
				_ = genIntCoder.Decode(dec)
			}
		})
		b.Run(fmt.Sprintf("int/size_%d/reflective_decode", size), func(b *testing.B) {
			b.ReportAllocs()
			dec := NewDecoder(encIntRef)
			for b.Loop() {
				dec.data = encIntRef
				_ = refIntCoder.Decode(dec)
			}
		})

		// []string
		strData := make([]string, size)
		for i := range strData {
			strData[i] = fmt.Sprintf("value-%d", i)
		}
		genStrCoder := MakeSliceCoder[string](nil)
		refStrCoder := makeSliceCoder[[]string](reflect.TypeFor[[]string]()).(Coder[[]string])
		encStrGen := Encode(genStrCoder, strData)
		encStrRef := Encode(refStrCoder, strData)

		b.Run(fmt.Sprintf("string/size_%d/generic_encode", size), func(b *testing.B) {
			b.ReportAllocs()
			enc := NewEncoder()
			for b.Loop() {
				enc.Reset(0)
				genStrCoder.Encode(enc, strData)
			}
		})
		b.Run(fmt.Sprintf("string/size_%d/reflective_encode", size), func(b *testing.B) {
			b.ReportAllocs()
			enc := NewEncoder()
			for b.Loop() {
				enc.Reset(0)
				refStrCoder.Encode(enc, strData)
			}
		})
		b.Run(fmt.Sprintf("string/size_%d/generic_decode", size), func(b *testing.B) {
			b.ReportAllocs()
			dec := NewDecoder(encStrGen)
			for b.Loop() {
				dec.data = encStrGen
				_ = genStrCoder.Decode(dec)
			}
		})
		b.Run(fmt.Sprintf("string/size_%d/reflective_decode", size), func(b *testing.B) {
			b.ReportAllocs()
			dec := NewDecoder(encStrRef)
			for b.Loop() {
				dec.data = encStrRef
				_ = refStrCoder.Decode(dec)
			}
		})
	}
}

type benchmarkRecord struct {
	ID    int
	Name  string
	Score float64
}

type genericBenchmarkRecordCoder struct{}

func (genericBenchmarkRecordCoder) Encode(enc *Encoder, v benchmarkRecord) {
	enc.Varint(3)
	enc.Varint(uint64(v.ID))
	enc.StringUtf8(v.Name)
	enc.Double(v.Score)
}

func (genericBenchmarkRecordCoder) Decode(dec *Decoder) benchmarkRecord {
	_ = dec.Varint()
	return benchmarkRecord{
		ID:    int(dec.Varint()),
		Name:  dec.StringUtf8(),
		Score: dec.Double(),
	}
}

func BenchmarkStructCoder(b *testing.B) {
	rec := benchmarkRecord{
		ID:    12345,
		Name:  "test-record-name-12345",
		Score: 98.7654,
	}

	genCoder := genericBenchmarkRecordCoder{}
	refCoder := makeRowCoder[benchmarkRecord](reflect.TypeFor[benchmarkRecord]()).(Coder[benchmarkRecord])

	encGen := Encode(genCoder, rec)
	encRef := Encode(refCoder, rec)

	b.Run("generic_encode", func(b *testing.B) {
		b.ReportAllocs()
		enc := NewEncoder()
		for b.Loop() {
			enc.Reset(0)
			genCoder.Encode(enc, rec)
		}
	})
	b.Run("reflective_encode", func(b *testing.B) {
		b.ReportAllocs()
		enc := NewEncoder()
		for b.Loop() {
			enc.Reset(0)
			refCoder.Encode(enc, rec)
		}
	})
	b.Run("generic_decode", func(b *testing.B) {
		b.ReportAllocs()
		dec := NewDecoder(encGen)
		for b.Loop() {
			dec.data = encGen
			_ = genCoder.Decode(dec)
		}
	})
	b.Run("reflective_decode", func(b *testing.B) {
		b.ReportAllocs()
		dec := NewDecoder(encRef)
		for b.Loop() {
			dec.data = encRef
			_ = refCoder.Decode(dec)
		}
	})
}

func TestPaneInfo(t *testing.T) {
	tests := []struct {
		name      string
		pane      PaneInfo
		wantBytes []byte
	}{
		{
			name:      "no firing pane (0x0F)",
			pane:      NoFiringPane,
			wantBytes: []byte{0x0F},
		},
		{
			name: "single byte on-time first and last pane (0x07)",
			pane: PaneInfo{
				Timing:              TimingOnTime,
				IsFirst:             true,
				IsLast:              true,
				Index:               0,
				NonSpeculativeIndex: 0,
			},
			wantBytes: []byte{0x07},
		},
		{
			name: "single byte early first pane (0x01)",
			pane: PaneInfo{
				Timing:              TimingEarly,
				IsFirst:             true,
				IsLast:              false,
				Index:               0,
				NonSpeculativeIndex: 0,
			},
			wantBytes: []byte{0x01},
		},
		{
			name: "single byte late last pane (0x0A)",
			pane: PaneInfo{
				Timing:              TimingLate,
				IsFirst:             false,
				IsLast:              true,
				Index:               0,
				NonSpeculativeIndex: 0,
			},
			wantBytes: []byte{0x0A},
		},
		{
			name:      "1-varint early pane (index 7, nonSpec -1, isFirst true)",
			pane:      PaneEarly(7, true),
			wantBytes: []byte{0x11, 0x07},
		},
		{
			name:      "1-varint on-time pane (index 5, nonSpec 5, isFirst false, isLast false)",
			pane:      PaneOnTime(5, false, false),
			wantBytes: []byte{0x14, 0x05},
		},
		{
			name: "2-varint on-time pane matching standard coders test vector (false, true, ON_TIME, 30, 40)",
			pane: PaneInfo{
				Timing:              TimingOnTime,
				IsFirst:             false,
				IsLast:              true,
				Index:               30,
				NonSpeculativeIndex: 40,
			},
			wantBytes: []byte{0x26, 0x1E, 0x28},
		},
		{
			name:      "2-varint late pane (false, true, LATE, 100, 20)",
			pane:      PaneLate(100, 20, true),
			wantBytes: []byte{0x2A, 0x64, 0x14},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.Pane(tc.pane)
			if got := enc.Data(); !bytes.Equal(got, tc.wantBytes) {
				t.Errorf("Encode Pane(%+v) = %x, want %x", tc.pane, got, tc.wantBytes)
			}
			dec := NewDecoder(tc.wantBytes)
			gotDecoded := dec.Pane()
			if gotDecoded != tc.pane {
				t.Errorf("Decode Pane(%x) = %+v, want %+v", tc.wantBytes, gotDecoded, tc.pane)
			}
		})
	}
}

func TestWindowedValueHeader(t *testing.T) {
	tests := []struct {
		name      string
		eventTime time.Time
		windows   []testWindow
		pane      PaneInfo
	}{
		{
			name:      "single window with NoFiringPane",
			eventTime: time.UnixMilli(1234567890),
			windows:   []testWindow{{val: 42}},
			pane:      NoFiringPane,
		},
		{
			name:      "multiple windows with on-time pane",
			eventTime: time.UnixMilli(9876543210),
			windows:   []testWindow{{val: 10}, {val: 20}, {val: 30}},
			pane:      PaneOnTime(3, false, true),
		},
		{
			name:      "zero windows with early pane",
			eventTime: time.UnixMilli(1000),
			windows:   []testWindow{},
			pane:      PaneEarly(1, true),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := NewEncoder()
			EncodeWindowedValueHeader(enc, tc.eventTime, tc.windows, tc.pane)
			dec := NewDecoder(enc.Data())
			gotTime, gotWins, gotPane := DecodeWindowedValueHeader[testWindow](dec)
			if gotTime.UnixMilli() != tc.eventTime.UnixMilli() {
				t.Errorf("got time = %v, want %v", gotTime, tc.eventTime)
			}
			if len(gotWins) != len(tc.windows) {
				t.Fatalf("got %d windows, want %d", len(gotWins), len(tc.windows))
			}
			for i := range gotWins {
				if gotWins[i].val != tc.windows[i].val {
					t.Errorf("window[%d] val = %v, want %v", i, gotWins[i].val, tc.windows[i].val)
				}
			}
			if gotPane != tc.pane {
				t.Errorf("got pane = %+v, want %+v", gotPane, tc.pane)
			}
		})
	}
}

