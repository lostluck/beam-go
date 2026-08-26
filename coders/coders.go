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

// Package coders is a pair of convenience handles for encoding and decoding
// values to [][byte].
package coders

import (
	"fmt"
	"reflect"
	"time"

	"golang.org/x/exp/constraints"
)

// Decode is a convenience function for decoding data using a coder.
func Decode[E any](coder Coder[E], data []byte) E {
	dec := NewDecoder(data)
	return coder.Decode(dec)
}

// Encode is a convenience function for encoding a value using a coder.
func Encode[E any](coder Coder[E], val E) []byte {
	enc := NewEncoder()
	coder.Encode(enc, val)
	return enc.Data()
}

// Coder represents a coder for a specific type.
type Coder[E any] interface {
	Encode(enc *Encoder, v E)
	Decode(dec *Decoder) E
}

// Codable represents types that know how to code themselves.
type Codable interface {
	Encode(enc *Encoder)
	Decode(dec *Decoder)
}

// MakeSliceCoder returns a fully typed generic Coder[[]E] with zero reflection during Encode/Decode.
func MakeSliceCoder[E any](elemCoder Coder[E]) Coder[[]E] {
	if elemCoder == nil {
		elemCoder = MakeCoder[E]()
	}
	return &sliceCoder[E]{elemCoder: elemCoder}
}

// MakeCoder is a convenience function for primitive coders access.
func MakeCoder[E any]() Coder[E] {
	var e E
	a := any(e)
	switch a.(type) {
	case bool:
		return any(boolCoder{}).(Coder[E])
	case int:
		return any(varintCoder[int]{}).(Coder[E])
	case int8:
		return any(varintCoder[int8]{}).(Coder[E])
	case int16:
		return any(varintCoder[int16]{}).(Coder[E])
	case int32:
		return any(varintCoder[int32]{}).(Coder[E])
	case int64:
		return any(varintCoder[int64]{}).(Coder[E])
	case uint:
		return any(varintCoder[uint]{}).(Coder[E])
	case uint8:
		return any(byteCoder{}).(Coder[E])
	case uint16:
		return any(varintCoder[uint16]{}).(Coder[E])
	case uint32:
		return any(varintCoder[uint32]{}).(Coder[E])
	case uint64:
		return any(varintCoder[uint64]{}).(Coder[E])
	case float32:
		return any(floatCoder{}).(Coder[E])
	case float64:
		return any(doubleCoder{}).(Coder[E])
	case complex64:
		return any(complex64Coder{}).(Coder[E])
	case complex128:
		return any(complex128Coder{}).(Coder[E])
	case string:
		return any(stringCoder{}).(Coder[E])
	case []byte:
		return any(bytesCoder{}).(Coder[E])
	case time.Time:
		return any(timeCoder{}).(Coder[E])
	case []bool:
		return any(MakeSliceCoder(MakeCoder[bool]())).(Coder[E])
	case []int:
		return any(MakeSliceCoder(MakeCoder[int]())).(Coder[E])
	case []int8:
		return any(MakeSliceCoder(MakeCoder[int8]())).(Coder[E])
	case []int16:
		return any(MakeSliceCoder(MakeCoder[int16]())).(Coder[E])
	case []int32:
		return any(MakeSliceCoder(MakeCoder[int32]())).(Coder[E])
	case []int64:
		return any(MakeSliceCoder(MakeCoder[int64]())).(Coder[E])
	case []uint:
		return any(MakeSliceCoder(MakeCoder[uint]())).(Coder[E])
	case []uint16:
		return any(MakeSliceCoder(MakeCoder[uint16]())).(Coder[E])
	case []uint32:
		return any(MakeSliceCoder(MakeCoder[uint32]())).(Coder[E])
	case []uint64:
		return any(MakeSliceCoder(MakeCoder[uint64]())).(Coder[E])
	case []float32:
		return any(MakeSliceCoder(MakeCoder[float32]())).(Coder[E])
	case []float64:
		return any(MakeSliceCoder(MakeCoder[float64]())).(Coder[E])
	case []complex64:
		return any(MakeSliceCoder(MakeCoder[complex64]())).(Coder[E])
	case []complex128:
		return any(MakeSliceCoder(MakeCoder[complex128]())).(Coder[E])
	case []string:
		return any(MakeSliceCoder(MakeCoder[string]())).(Coder[E])
	case [][]byte:
		return any(MakeSliceCoder(MakeCoder[[]byte]())).(Coder[E])
	case []time.Time:
		return any(MakeSliceCoder(MakeCoder[time.Time]())).(Coder[E])
	}
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Struct {
		return makeRowCoder[E](rt).(Coder[E])
	}
	if rt.Kind() == reflect.Slice {
		return makeSliceCoder[E](rt).(Coder[E])
	}
	return makeCoder(rt).(Coder[E])
}

// makeCoder works around generic coding.
func makeCoder(rt reflect.Type) any {
	switch rt.Kind() {
	case reflect.Bool:
		return boolCoder{}
	case reflect.Int:
		return varintCoder[int]{}
	case reflect.Int8:
		return varintCoder[int8]{}
	case reflect.Int16:
		return varintCoder[int16]{}
	case reflect.Int32:
		return varintCoder[int32]{}
	case reflect.Int64:
		return varintCoder[int64]{}
	case reflect.Uint:
		return varintCoder[uint]{}
	case reflect.Uint8:
		return byteCoder{}
	case reflect.Uint16:
		return varintCoder[uint16]{}
	case reflect.Uint32:
		return varintCoder[uint32]{}
	case reflect.Uint64:
		return varintCoder[uint64]{}
	case reflect.Float32:
		return floatCoder{}
	case reflect.Float64:
		return doubleCoder{}
	case reflect.Complex64:
		return complex64Coder{}
	case reflect.Complex128:
		return complex128Coder{}
	case reflect.String:
		return stringCoder{}
	case reflect.Slice:
		switch rt.Elem().Kind() {
		case reflect.Uint8:
			return bytesCoder{}
		}
	}
	// Returning nil since type assertion elsewhere will provide better information
	// to the developer.
	return nil
}

func makeRowCoder[E any](rt reflect.Type) any {
	return buildRowCoder(&rowStructCoder[E]{}, rt)
}

var (
	rtTimeTime = reflect.TypeFor[time.Time]()
)

// buildRowCoder abstracts between the generic top level,
// and the interface/reflect based nested levels for Row coders.
func buildRowCoder[C rowStructCoderBuilder](c C, rt reflect.Type) C {
	switch rt {
	case rtTimeTime:
		c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
			t := rv.Interface().(time.Time)
			mar, _ := t.MarshalText()
			enc.Bytes(mar)
		})
		c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
			t := time.Time{}
			if err := t.UnmarshalText(dec.Bytes()); err != nil {
				panic(makeDecodeError("error decoding time.Time: %w", err))
			}
			rv.Set(reflect.ValueOf(t))
		})
		return c
	}
	// TODO: move this to be generated from the Schema + the user type.
	// Also need to deal with length prefixing. Ugh.
	for sf := range rt.Fields() {
		if !sf.IsExported() {
			// Put in dummy handlers for unexported fields.
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {})
			continue
		}
		switch sf.Type.Kind() {
		case reflect.Bool:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Bool(rv.Bool())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetBool(dec.Bool())
			})
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Varint(uint64(rv.Int()))
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetInt(int64(dec.Varint()))
			})
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Varint(rv.Uint())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetUint(dec.Varint())
			})
		case reflect.Uint8:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Byte(byte(rv.Uint()))
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetUint(uint64(dec.Byte()))
			})
		case reflect.Float32:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Float(float32(rv.Float()))
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetFloat(float64(dec.Float()))
			})
		case reflect.Float64:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Double(rv.Float())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetFloat(dec.Double())
			})
		case reflect.Complex64:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Complex64(complex64(rv.Complex()))
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetComplex(complex128(dec.Complex64()))
			})
		case reflect.Complex128:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Complex128(rv.Complex())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetComplex(dec.Complex128())
			})
		case reflect.String:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.StringUtf8(rv.String())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetString(dec.StringUtf8())
			})
		case reflect.Slice:
			if sf.Type.Elem().Kind() == reflect.Uint8 {
				c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
					enc.Bytes(rv.Bytes())
				})
				c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
					rv.SetBytes(dec.Bytes())
				})
			} else {
				elemEnc, elemDec := buildElemFuncs(sf.Type.Elem())
				c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
					if !rv.IsValid() || rv.IsNil() {
						enc.Int32(0)
						return
					}
					n := rv.Len()
					enc.Int32(int32(n))
					for i := range n {
						elemEnc(enc, rv.Index(i))
					}
				})
				c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
					n := int(dec.Int32())
					if n < 0 {
						panic(makeDecodeError("invalid slice length: %d", n))
					}
					res := reflect.MakeSlice(sf.Type, n, n)
					for i := range n {
						res.Index(i).Set(elemDec(dec))
					}
					rv.Set(res)
				})
			}
		case reflect.Struct:
			nrc := buildRowCoder(&rowStructCoderNested{rt: sf.Type}, sf.Type)
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				nrc.Encode(enc, rv)
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.Set(nrc.Decode(dec))
			})
		default:
			panic("row field type unknown:" + sf.Type.Kind().String() + " for type " + rt.Name())
		}
	}
	return c
}

func buildElemFuncs(elemRT reflect.Type) (func(enc *Encoder, rv reflect.Value), func(dec *Decoder) reflect.Value) {
	if elemRT == rtTimeTime {
		return func(enc *Encoder, rv reflect.Value) {
				t := rv.Interface().(time.Time)
				mar, _ := t.MarshalText()
				enc.Bytes(mar)
			}, func(dec *Decoder) reflect.Value {
				var t time.Time
				if err := t.UnmarshalText(dec.Bytes()); err != nil {
					panic(makeDecodeError("error decoding time.Time: %w", err))
				}
				return reflect.ValueOf(t)
			}
	}
	switch elemRT.Kind() {
	case reflect.Bool:
		return func(enc *Encoder, rv reflect.Value) { enc.Bool(rv.Bool()) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Bool()).Convert(elemRT) }
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(enc *Encoder, rv reflect.Value) { enc.Varint(uint64(rv.Int())) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(int64(dec.Varint())).Convert(elemRT) }
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(enc *Encoder, rv reflect.Value) { enc.Varint(rv.Uint()) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Varint()).Convert(elemRT) }
	case reflect.Uint8:
		return func(enc *Encoder, rv reflect.Value) { enc.Byte(byte(rv.Uint())) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Byte()).Convert(elemRT) }
	case reflect.Float32:
		return func(enc *Encoder, rv reflect.Value) { enc.Float(float32(rv.Float())) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Float()).Convert(elemRT) }
	case reflect.Float64:
		return func(enc *Encoder, rv reflect.Value) { enc.Double(rv.Float()) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Double()).Convert(elemRT) }
	case reflect.Complex64:
		return func(enc *Encoder, rv reflect.Value) { enc.Complex64(complex64(rv.Complex())) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Complex64()).Convert(elemRT) }
	case reflect.Complex128:
		return func(enc *Encoder, rv reflect.Value) { enc.Complex128(rv.Complex()) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Complex128()).Convert(elemRT) }
	case reflect.String:
		return func(enc *Encoder, rv reflect.Value) { enc.StringUtf8(rv.String()) },
			func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.StringUtf8()).Convert(elemRT) }
	case reflect.Slice:
		if elemRT.Elem().Kind() == reflect.Uint8 {
			return func(enc *Encoder, rv reflect.Value) { enc.Bytes(rv.Bytes()) },
				func(dec *Decoder) reflect.Value { return reflect.ValueOf(dec.Bytes()) }
		}
		nestedElemEnc, nestedElemDec := buildElemFuncs(elemRT.Elem())
		return func(enc *Encoder, rv reflect.Value) {
				if !rv.IsValid() || rv.IsNil() {
					enc.Int32(0)
					return
				}
				n := rv.Len()
				enc.Int32(int32(n))
				for i := range n {
					nestedElemEnc(enc, rv.Index(i))
				}
			}, func(dec *Decoder) reflect.Value {
				n := int(dec.Int32())
				if n < 0 {
					panic(makeDecodeError("invalid slice length: %d", n))
				}
				res := reflect.MakeSlice(elemRT, n, n)
				for i := range n {
					res.Index(i).Set(nestedElemDec(dec))
				}
				return res
			}
	case reflect.Struct:
		nrc := buildRowCoder(&rowStructCoderNested{rt: elemRT}, elemRT)
		return func(enc *Encoder, rv reflect.Value) { nrc.Encode(enc, rv) },
			func(dec *Decoder) reflect.Value { return nrc.Decode(dec) }
	default:
		panic("slice element type unknown: " + elemRT.Kind().String() + " for type " + elemRT.Name())
	}
}

type rowStructCoderBuilder interface {
	appendEncoder(func(enc *Encoder, rv reflect.Value))
	appendDecoder(func(dec *Decoder, rv reflect.Value))
}

type rowStructCoder[T any] struct {
	fieldEncoders []func(enc *Encoder, rv reflect.Value)
	fieldDecoders []func(dec *Decoder, rv reflect.Value)
}

func (c *rowStructCoder[T]) appendEncoder(encFn func(enc *Encoder, rv reflect.Value)) {
	c.fieldEncoders = append(c.fieldEncoders, encFn)
}

func (c *rowStructCoder[T]) appendDecoder(decFn func(dec *Decoder, rv reflect.Value)) {
	c.fieldDecoders = append(c.fieldDecoders, decFn)
}

func (c *rowStructCoder[T]) Encode(enc *Encoder, v T) {
	rv := reflect.ValueOf(v)
	enc.Varint(uint64(rv.NumField()))
	for i := 0; i < rv.NumField(); i++ {
		c.fieldEncoders[i](enc, rv.Field(i))
	}
}

func (c *rowStructCoder[T]) Decode(dec *Decoder) T {
	var v T
	rv := reflect.ValueOf(&v).Elem()
	i := 0
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("field %v:\n%v", i, e))
		}
	}()
	n := dec.Varint()
	if int(n) != rv.NumField() {
		panic(fmt.Sprintf("row value got %v fields want %v fields for a %v", n, rv.NumField(), rv.Type()))
	}
	for ; i < rv.NumField(); i++ {
		c.fieldDecoders[i](dec, rv.Field(i))
	}
	return rv.Interface().(T)
}

type rowStructCoderNested struct {
	rt            reflect.Type
	fieldEncoders []func(enc *Encoder, rv reflect.Value)
	fieldDecoders []func(dec *Decoder, rv reflect.Value)
}

func (c *rowStructCoderNested) appendEncoder(encFn func(enc *Encoder, rv reflect.Value)) {
	c.fieldEncoders = append(c.fieldEncoders, encFn)
}

func (c *rowStructCoderNested) appendDecoder(decFn func(dec *Decoder, rv reflect.Value)) {
	c.fieldDecoders = append(c.fieldDecoders, decFn)
}

func (c *rowStructCoderNested) Encode(enc *Encoder, rv reflect.Value) {
	enc.Varint(uint64(rv.NumField()))
	switch rv.Type() {
	case rtTimeTime:
		c.fieldEncoders[0](enc, rv)
		return
	}
	for i := 0; i < rv.NumField(); i++ {
		c.fieldEncoders[i](enc, rv.Field(i))
	}
}

func (c *rowStructCoderNested) Decode(dec *Decoder) reflect.Value {
	rv := reflect.New(c.rt).Elem()
	i := 0
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("field %v:\n%v", i, e))
		}
	}()
	n := dec.Varint()
	switch rv.Type() {
	case rtTimeTime:
		c.fieldDecoders[0](dec, rv)
		return rv
	}
	if int(n) != rv.NumField() {
		panic(fmt.Sprintf("row value got %v fields want %v fields for a %v", n, rv.NumField(), rv.Type()))
	}
	for ; i < rv.NumField(); i++ {
		c.fieldDecoders[i](dec, rv.Field(i))
	}
	return rv
}

func makeSliceCoder[E any](rt reflect.Type) any {
	elemEnc, elemDec := buildElemFuncs(rt.Elem())
	return &reflectSliceCoder[E]{
		sliceType: rt,
		elemEnc:   elemEnc,
		elemDec:   elemDec,
	}
}

type sliceCoder[E any] struct {
	elemCoder Coder[E]
}

func (c *sliceCoder[E]) Encode(enc *Encoder, v []E) {
	enc.Int32(int32(len(v)))
	for _, elem := range v {
		c.elemCoder.Encode(enc, elem)
	}
}

func (c *sliceCoder[E]) Decode(dec *Decoder) []E {
	n := dec.Int32()
	if n < 0 {
		panic(makeDecodeError("invalid slice length: %d", n))
	}
	res := make([]E, n)
	for i := range res {
		res[i] = c.elemCoder.Decode(dec)
	}
	return res
}

type reflectSliceCoder[T any] struct {
	sliceType reflect.Type
	elemEnc   func(enc *Encoder, rv reflect.Value)
	elemDec   func(dec *Decoder) reflect.Value
}

func (c *reflectSliceCoder[T]) Encode(enc *Encoder, v T) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || rv.IsNil() {
		enc.Int32(0)
		return
	}
	n := rv.Len()
	enc.Int32(int32(n))
	for i := range n {
		c.elemEnc(enc, rv.Index(i))
	}
}

func (c *reflectSliceCoder[T]) Decode(dec *Decoder) T {
	n := int(dec.Int32())
	if n < 0 {
		panic(makeDecodeError("invalid slice length: %d", n))
	}
	res := reflect.MakeSlice(c.sliceType, n, n)
	for i := range n {
		res.Index(i).Set(c.elemDec(dec))
	}
	return res.Interface().(T)
}

type varintCoder[T constraints.Integer] struct{}

func (varintCoder[T]) Encode(enc *Encoder, v T) {
	enc.Varint(uint64(v))
}

func (varintCoder[T]) Decode(dec *Decoder) T {
	return T(dec.Varint())
}

type byteCoder struct{}

func (byteCoder) Encode(enc *Encoder, v byte) {
	enc.Byte(v)
}

func (byteCoder) Decode(dec *Decoder) byte {
	return dec.Byte()
}

type bytesCoder struct{}

func (bytesCoder) Encode(enc *Encoder, v []byte) {
	enc.Bytes(v)
}

func (bytesCoder) Decode(dec *Decoder) []byte {
	return dec.Bytes()
}

type stringCoder struct{}

func (stringCoder) Encode(enc *Encoder, v string) {
	enc.StringUtf8(v)
}

func (stringCoder) Decode(dec *Decoder) string {
	return dec.StringUtf8()
}

type floatCoder struct{}

func (floatCoder) Encode(enc *Encoder, v float32) {
	enc.Float(v)
}

func (floatCoder) Decode(dec *Decoder) float32 {
	return dec.Float()
}

type doubleCoder struct{}

func (doubleCoder) Encode(enc *Encoder, v float64) {
	enc.Double(v)
}

func (doubleCoder) Decode(dec *Decoder) float64 {
	return dec.Double()
}

type complex64Coder struct{}

func (complex64Coder) Encode(enc *Encoder, v complex64) {
	enc.Complex64(v)
}

func (complex64Coder) Decode(dec *Decoder) complex64 {
	return dec.Complex64()
}

type complex128Coder struct{}

func (complex128Coder) Encode(enc *Encoder, v complex128) {
	enc.Complex128(v)
}

func (complex128Coder) Decode(dec *Decoder) complex128 {
	return dec.Complex128()
}

type boolCoder struct{}

func (boolCoder) Encode(enc *Encoder, v bool) {
	enc.Bool(v)
}

func (boolCoder) Decode(dec *Decoder) bool {
	return dec.Bool()
}

type timeCoder struct{}

func (timeCoder) Encode(enc *Encoder, v time.Time) {
	mar, _ := v.MarshalText()
	enc.Bytes(mar)
}

func (timeCoder) Decode(dec *Decoder) time.Time {
	var t time.Time
	if err := t.UnmarshalText(dec.Bytes()); err != nil {
		panic(makeDecodeError("error decoding time.Time: %w", err))
	}
	return t
}
