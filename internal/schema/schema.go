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

// Package schema handles converting a Beam schema to a Row and back again.
package schema

import (
	"fmt"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// RowValue represents a dynamic row type that is defined by a Beam Row Schema.
type RowValue struct {
	numFields uint64
	nils      []byte // packed bit fields.

	nameToNum map[string]int
	fields    []fieldValue
}

type fieldValue struct {
	Val any
}

// Sets the value for the given field, and updates the nil bit.
func (v *RowValue) Set(fieldname string, val any) {
	fnum := v.nameToNum[fieldname]
	v.fields[fnum].Val = val
	if val == nil {
		v.nils = clearBit(v.nils, fnum)
	} else {
		v.nils = setBit(v.nils, fnum)
	}
}

// Sets the bit at pos in the byte b.
func setBit(nils []byte, f int) []byte {
	i, pos := f/8, f%8
	nils[i] |= (1 << pos)
	return nils
}

// Clears the bit for field  in b.
func clearBit(nils []byte, f int) []byte {
	i, pos := f/8, f%8
	mask := byte(^(1 << pos))
	nils[i] &= mask
	return nils
}

// RowValueCoder is a stateless coder for a provided schema to dynamic RowValues.
type RowValueCoder struct {
	schema   *pipepb.Schema
	rawCoder coders.Coder[any]
}

var _ coders.Coder[RowValue] = (*RowValueCoder)(nil)

// Decode will decode bytes into a RowValue
func (c *RowValueCoder) Decode(dec *coders.Decoder) RowValue {
	return c.rawCoder.Decode(dec).(RowValue)
}

func (c *RowValueCoder) Encode(enc *coders.Encoder, val RowValue) {
	c.rawCoder.Encode(enc, val)
}

// ToCoder takes a schema a produces a coder for RowValues of that schema.
func ToCoder(schema *pipepb.Schema) coders.Coder[RowValue] {
	if schema.GetEncodingPositionsSet() {
		panic("changed encoding positions not yet supported")
	}
	rawCoder := buildSchemaCoder(schema)
	return &RowValueCoder{
		schema:   proto.Clone(schema).(*pipepb.Schema),
		rawCoder: rawCoder,
	}
}

func buildSchemaCoder(schema *pipepb.Schema) coders.Coder[any] {
	c := &rowCoder{
		namesToNums: make(map[string]int),
	}
	for i, f := range schema.GetFields() {
		c.namesToNums[f.GetName()] = i
		ft := f.GetType()
		c.nullable = append(c.nullable, ft.GetNullable())
		c.fields = append(c.fields, buildFieldCoder(ft))
	}
	return c
}

func buildFieldCoder(ft *pipepb.FieldType) coders.Coder[any] {
	switch ti := ft.GetTypeInfo().(type) {
	case *pipepb.FieldType_AtomicType:
		return buildAtomicCoder(ti.AtomicType)
	case *pipepb.FieldType_ArrayType:
		return buildArrayCoder(ti.ArrayType.GetElementType())
	case *pipepb.FieldType_IterableType:
		return buildArrayCoder(ti.IterableType.GetElementType())
	case *pipepb.FieldType_MapType:
		return buildMapCoder(ti.MapType)
	case *pipepb.FieldType_LogicalType:
		return buildLogicalCoder(ti.LogicalType)
	case *pipepb.FieldType_RowType:
		return buildSchemaCoder(ti.RowType.GetSchema())
	default:
		panic(fmt.Sprintf("unimplemented field type: %v", prototext.Format(ft)))
	}
}

func buildAtomicCoder(atype pipepb.AtomicType) coders.Coder[any] {
	switch atype {
	case pipepb.AtomicType_BOOLEAN:
		return wrapCoder(coders.MakeCoder[bool]())
	case pipepb.AtomicType_BYTE:
		return wrapCoder(coders.MakeCoder[byte]())
	case pipepb.AtomicType_BYTES:
		return wrapCoder(coders.MakeCoder[[]byte]())
	case pipepb.AtomicType_DOUBLE:
		return wrapCoder(coders.MakeCoder[float64]())
	case pipepb.AtomicType_FLOAT:
		return wrapCoder(coders.MakeCoder[float32]())
	case pipepb.AtomicType_INT16:
		return wrapCoder(coders.MakeCoder[int16]())
	case pipepb.AtomicType_INT32:
		return wrapCoder(coders.MakeCoder[int32]())
	case pipepb.AtomicType_INT64:
		return wrapCoder(coders.MakeCoder[int64]())

	case pipepb.AtomicType_STRING:
		return wrapCoder(coders.MakeCoder[string]())
	default:
		panic(fmt.Sprintf("unimplemented atomic field type: %v", atype))
	}
}

func buildArrayCoder(ftype *pipepb.FieldType) coders.Coder[any] {
	fc := buildFieldCoder(ftype)
	if ftype.GetNullable() {
		fc = &nullableCoder{wrap: fc}
	}
	return &arrayCoder{field: fc}
}

func buildMapCoder(maptype *pipepb.MapType) coders.Coder[any] {
	kc := buildFieldCoder(maptype.GetKeyType())
	vc := buildFieldCoder(maptype.GetValueType())
	if maptype.GetValueType().GetNullable() {
		vc = &nullableCoder{wrap: vc}
	}
	return &mapCoder{key: kc, value: vc}
}

func buildLogicalCoder(logtype *pipepb.LogicalType) coders.Coder[any] {
	urn := logtype.GetUrn()
	switch urn {
	case "beam:logical_type:millis_instant:v1":
		// Non-conforming Representation.
		// Claims to be INT64 which is varint encoded,
		// but uses a raw big endian encoding for Int64s, like for timestamps.
		return &int64Coder{}
	case "beam:logical_type:decimal:v1":
		// Non-conforming Representation.
		// It claims "bytes", but is a varint of
		// the scale, followed by a followed by a bytes coding of the raw big
		// integer.
		// It's not a row structure either.
		return &decimalCoder{}

	default:
		// Do nothing special, and just use the Representation raw.
		return buildFieldCoder(logtype.GetRepresentation())
	}
}

type nullableCoder struct {
	wrap coders.Coder[any]
}

// Decode implements coders.Coder.
func (c *nullableCoder) Decode(dec *coders.Decoder) any {
	if dec.Byte() == 0x00 {
		return nil
	}
	return c.wrap.Decode(dec)
}

// Encode implements coders.Coder.
func (c *nullableCoder) Encode(enc *coders.Encoder, v any) {
	if v == nil {
		enc.Byte(0x00)
		return
	}
	enc.Byte(0x01)
	c.wrap.Encode(enc, v)
}

type arrayCoder struct {
	field coders.Coder[any]
}

// Decode implements coders.Coder.
func (c *arrayCoder) Decode(dec *coders.Decoder) any {
	n := dec.Uint32()
	arr := make([]any, 0, n)
	for range n {
		arr = append(arr, c.field.Decode(dec))
	}
	return arr
}

// Encode implements coders.Coder.
func (c *arrayCoder) Encode(enc *coders.Encoder, val any) {
	arr := val.([]any)
	enc.Int32(int32(len(arr)))
	for _, v := range arr {
		c.field.Encode(enc, v)
	}
}

type mapCoder struct {
	key, value coders.Coder[any]
}

func (c *mapCoder) Decode(dec *coders.Decoder) any {
	n := dec.Uint32()
	m := make(map[any]any, n)
	for range n {
		k := c.key.Decode(dec)
		v := c.value.Decode(dec)
		m[k] = v
	}
	return m
}

func (c *mapCoder) Encode(enc *coders.Encoder, val any) {
	m := val.(map[any]any)
	enc.Int32(int32(len(m)))
	for k, v := range m {
		c.key.Encode(enc, k)
		c.value.Encode(enc, v)
	}
}

// rowCoder is a `coders.Coder[any]` that produces a RowValue wrapped in an interface.
type rowCoder struct {
	namesToNums map[string]int // precomputed index.
	fields      []coders.Coder[any]
	nullable    []bool
}

var _ coders.Coder[any] = (*rowCoder)(nil)

// Decode implements coders.Coder.
func (c *rowCoder) Decode(dec *coders.Decoder) any {
	// Varint for the Number of Row field in this encoded row.
	nf := dec.Varint()
	// Varint for the length of the packed bit field for nils.
	// This is just a normal Byte buffer decoder.
	nils := dec.Bytes()

	row := RowValue{
		numFields: nf,
		nils:      nils,
		nameToNum: c.namesToNums,
		fields:    make([]fieldValue, nf),
	}
	for i, fc := range c.fields {
		if c.nullable[i] && isFieldNil(row.nils, i) {
			continue
		}
		row.fields[i].Val = fc.Decode(dec)
	}
	return row
}

// Encode implements coders.Coder.
func (c *rowCoder) Encode(enc *coders.Encoder, v any) {
	row := v.(RowValue)
	// Row header has the number of fields
	enc.Varint(uint64(len(row.fields)))
	// Followed by the nil bits.
	enc.Bytes(row.nils)

	for i, fc := range c.fields {
		if c.nullable[i] && isFieldNil(row.nils, i) {
			continue
		}
		fc.Encode(enc, row.fields[i].Val)
	}
}

// isFieldNil examines the passed in packed bits nils buffer
// and returns true if the field at that index wasn't encoded
// and can be skipped in decoding.
func isFieldNil(nils []byte, f int) bool {
	i, b := f/8, f%8
	// https://github.com/apache/beam/issues/21232: The row header can elide trailing 0 bytes,
	// and we shouldn't care if there are trailing 0 bytes when doing a lookup.
	return i < len(nils) && len(nils) != 0 && (nils[i]>>uint8(b))&0x1 == 1
}

func wrapCoder[E any, C coders.Coder[E]](coder C) coders.Coder[any] {
	return &anyCoderAdapter[E, C]{wrapped: coder}
}

type anyCoderAdapter[E any, C coders.Coder[E]] struct {
	wrapped C
}

// Decode implements coders.Coder.
func (c *anyCoderAdapter[E, C]) Decode(dec *coders.Decoder) any {
	return c.wrapped.Decode(dec)
}

// Encode implements coders.Coder.
func (c *anyCoderAdapter[E, C]) Encode(enc *coders.Encoder, v any) {
	c.wrapped.Encode(enc, v.(E))
}

// Logical Coders

// int64Coder is a fixed sized bigEndian representation of an int64.
type int64Coder struct{}

// Decode implements coders.Coder.
func (c *int64Coder) Decode(dec *coders.Decoder) any {
	return dec.Int64()
}

// Encode implements coders.Coder.
func (c *int64Coder) Encode(enc *coders.Encoder, v any) {
	enc.Int64(v.(int64))
}

// decimal is a temporary measure for handling the decimal
// logical type WRT to parsing them.
// Improve logical type handling will be some other time.
type decimal struct {
	Scale    int64
	TwosComp []byte
}

type decimalCoder struct{}

func (c *decimalCoder) Decode(dec *coders.Decoder) any {
	scale := int64(dec.Varint())
	data := dec.Bytes()
	return decimal{Scale: scale, TwosComp: data}
}

func (c *decimalCoder) Encode(enc *coders.Encoder, val any) {
	deci := val.(decimal)
	enc.Varint(uint64(deci.Scale))
	enc.Bytes(deci.TwosComp)
}
