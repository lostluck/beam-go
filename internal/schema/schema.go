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
	"bytes"
	"fmt"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

// RowValue represents a dynamic row type that is defined by a Beam Row Schema.
type RowValue struct {
	numFields uint64
	nils      []byte // packed bit field representing which fields are nil.
	dirty     bool   // Dirty bit to copy nils on write.

	nameToNum map[string]int
	fields    []fieldValue
}

// Get the value for the given field, and updates the nil bit.
func (v *RowValue) Get(fieldname string) any {
	fnum := v.nameToNum[fieldname]
	return v.fields[fnum].Interface()
}

// Set the value for the given field, and updates the nil bit.
func (v *RowValue) Set(fieldname string, val any) {
	fnum := v.nameToNum[fieldname]
	v.fields[fnum] = valFromIface(val)
	if val == nil {
		v.setBit(fnum)
	} else {
		v.clearBit(fnum)
	}
}

// Sets the nil bit for field f.
func (v *RowValue) setBit(f int) {
	if !v.dirty {
		v.dirty = true
		v.nils = bytes.Clone(v.nils)
	}
	i, pos := f/8, f%8
	v.nils[i] |= (1 << pos)
}

// Clears the bit for field  in b.
func (v *RowValue) clearBit(f int) {
	if !v.dirty {
		v.dirty = true
		v.nils = bytes.Clone(v.nils)
	}
	i, pos := f/8, f%8
	mask := byte(^(1 << pos))
	v.nils[i] &= mask
}

// RowValueCoder is a stateless coder for a provided schema to dynamic RowValues.
type RowValueCoder struct {
	schema   *pipepb.Schema
	rawCoder *rowCoder
}

var _ coders.Coder[RowValue] = (*RowValueCoder)(nil)

// Decode will decode bytes into a RowValue
func (c *RowValueCoder) Decode(dec *coders.Decoder) RowValue {
	// We lift out the row coder implementation directly
	// to avoid extra allocations through interfaces.

	// Varint for the Number of Row field in this encoded row.
	nf := dec.Varint()
	// Varint for the length of the packed bit field for nils.
	// This is just a normal Byte buffer decoder.
	nils := dec.Bytes()

	row := RowValue{
		numFields: nf,
		nils:      nils,
		nameToNum: c.rawCoder.namesToNums,
		fields:    make([]fieldValue, nf),
	}
	for i, fc := range c.rawCoder.fields {
		if c.rawCoder.nullable[i] && isFieldNil(row.nils, i) {
			continue
		}
		row.fields[i] = fc.Decode(dec)
	}
	return row
}

// Encode will convert the RowValue into bytes based on this coder's schema.
func (c *RowValueCoder) Encode(enc *coders.Encoder, row RowValue) {
	// We lift out the row coder implementation directly
	// to avoid extra allocations through interfaces.

	// Row header has the number of fields
	enc.Varint(uint64(len(row.fields)))
	// Followed by the nil bits.
	enc.Bytes(row.nils)

	for i, fc := range c.rawCoder.fields {
		if c.rawCoder.nullable[i] && isFieldNil(row.nils, i) {
			continue
		}
		fc.Encode(enc, row.fields[i])
	}
}

// ToCoder takes a schema a produces a coder for RowValues of that schema.
func ToCoder(schema *pipepb.Schema) coders.Coder[RowValue] {
	if schema.GetEncodingPositionsSet() {
		panic("changed encoding positions not yet supported")
	}
	rawCoder := buildSchemaCoder(schema).(*rowCoder)
	return &RowValueCoder{
		schema:   proto.Clone(schema).(*pipepb.Schema),
		rawCoder: rawCoder,
	}
}

func buildSchemaCoder(schema *pipepb.Schema) coders.Coder[fieldValue] {
	c := &rowCoder{
		namesToNums: make(map[string]int),
		fields:      make([]coders.Coder[fieldValue], 0, len(schema.GetFields())),
		nullable:    make([]bool, 0, len(schema.GetFields())),
	}
	for i, f := range schema.GetFields() {
		c.namesToNums[f.GetName()] = i
		ft := f.GetType()
		c.nullable = append(c.nullable, ft.GetNullable())
		c.fields = append(c.fields, buildFieldCoder(ft))
	}
	return c
}

func buildFieldCoder(ft *pipepb.FieldType) coders.Coder[fieldValue] {
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

func buildAtomicCoder(atype pipepb.AtomicType) coders.Coder[fieldValue] {
	switch atype {
	case pipepb.AtomicType_BOOLEAN:
		return &boolCoder{}
	case pipepb.AtomicType_BYTE:
		return &byteCoder{}
	case pipepb.AtomicType_BYTES:
		return &bytesCoder{}
	case pipepb.AtomicType_DOUBLE:
		return &doubleCoder{}
	case pipepb.AtomicType_FLOAT:
		return &floatCoder{}
	case pipepb.AtomicType_INT16:
		return &varInt16Coder{}
	case pipepb.AtomicType_INT32:
		return &varInt32Coder{}
	case pipepb.AtomicType_INT64:
		return &varInt64Coder{}

	case pipepb.AtomicType_STRING:
		return &stringCoder{}
	default:
		panic(fmt.Sprintf("unimplemented atomic field type: %v", atype))
	}
}

type boolCoder struct{}

func (*boolCoder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Bool(v.num > 0)
}

func (*boolCoder) Decode(dec *coders.Decoder) fieldValue {
	if dec.Bool() {
		return fieldValue{typ: boolType, num: 1}
	}
	return fieldValue{typ: boolType, num: 0}
}

type byteCoder struct{}

func (*byteCoder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Byte(byte(v.num))
}

func (*byteCoder) Decode(dec *coders.Decoder) fieldValue {
	return fieldValue{typ: uint8Type, num: uint64(dec.Byte())}
}

type bytesCoder struct{}

func (*bytesCoder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Bytes(v.getBytes())
}

func (*bytesCoder) Decode(dec *coders.Decoder) fieldValue {
	return valueOfBytes(dec.Bytes())
}

type varInt16Coder struct{}

func (*varInt16Coder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Varint(v.num)
}

func (*varInt16Coder) Decode(dec *coders.Decoder) fieldValue {
	return fieldValue{typ: int16Type, num: dec.Varint()}
}

type varInt32Coder struct{}

func (*varInt32Coder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Varint(v.num)
}

func (*varInt32Coder) Decode(dec *coders.Decoder) fieldValue {
	return fieldValue{typ: int32Type, num: dec.Varint()}
}

type varInt64Coder struct{}

func (*varInt64Coder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Varint(v.num)
}

func (*varInt64Coder) Decode(dec *coders.Decoder) fieldValue {
	return fieldValue{typ: int64Type, num: dec.Varint()}
}

type stringCoder struct{}

func (*stringCoder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.StringUtf8(v.getString())
}

func (*stringCoder) Decode(dec *coders.Decoder) fieldValue {
	return valueOfString(dec.StringUtf8())
}

type doubleCoder struct{}

func (*doubleCoder) Encode(enc *coders.Encoder, v fieldValue) {
	// Using Uint64 directly instead of indirecting through multiple
	// passes of math.Float64frombits.
	enc.Uint64(v.num)
}

func (*doubleCoder) Decode(dec *coders.Decoder) fieldValue {
	// Using Uint64 directly instead of indirecting through multiple
	// passes of math.Float64frombits.
	return fieldValue{typ: float64Type, num: dec.Uint64()}
}

type floatCoder struct{}

func (*floatCoder) Encode(enc *coders.Encoder, v fieldValue) {
	// Using Uint32 directly instead of indirecting through multiple
	// passes of math.Float64frombits.
	enc.Uint32(uint32(v.num))
}

func (*floatCoder) Decode(dec *coders.Decoder) fieldValue {
	// Using Uint32 directly instead of indirecting through multiple
	// passes of math.Float64frombits.
	return fieldValue{typ: float32Type, num: uint64(dec.Uint32())}
}

func buildArrayCoder(ftype *pipepb.FieldType) coders.Coder[fieldValue] {
	fc := buildFieldCoder(ftype)
	if ftype.GetNullable() {
		fc = &nullableCoder{wrap: fc}
	}
	return &arrayCoder{field: fc}
}

func buildMapCoder(maptype *pipepb.MapType) coders.Coder[fieldValue] {
	kc := buildFieldCoder(maptype.GetKeyType())
	vc := buildFieldCoder(maptype.GetValueType())
	if maptype.GetValueType().GetNullable() {
		vc = &nullableCoder{wrap: vc}
	}
	return &mapCoder{key: kc, value: vc}
}

func buildLogicalCoder(logtype *pipepb.LogicalType) coders.Coder[fieldValue] {
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
	wrap coders.Coder[fieldValue]
}

// Decode implements coders.Coder.
func (c *nullableCoder) Decode(dec *coders.Decoder) fieldValue {
	if dec.Byte() == 0x00 {
		return fieldValue{typ: nilType}
	}
	return c.wrap.Decode(dec)
}

// Encode implements coders.Coder.
func (c *nullableCoder) Encode(enc *coders.Encoder, v fieldValue) {
	if v.typ == nilType {
		enc.Byte(0x00)
		return
	}
	enc.Byte(0x01)
	c.wrap.Encode(enc, v)
}

type arrayCoder struct {
	field coders.Coder[fieldValue]
}

// Decode implements coders.Coder.
func (c *arrayCoder) Decode(dec *coders.Decoder) fieldValue {
	n := dec.Uint32()
	arr := make([]fieldValue, 0, n)
	for range n {
		arr = append(arr, c.field.Decode(dec))
	}
	return fieldValue(valueOfIface(arr))
}

// Encode implements coders.Coder.
func (c *arrayCoder) Encode(enc *coders.Encoder, val fieldValue) {
	arr := val.getIface().([]fieldValue)
	enc.Int32(int32(len(arr)))
	for _, v := range arr {
		c.field.Encode(enc, v)
	}
}

type mapCoder struct {
	key, value coders.Coder[fieldValue]
}

func (c *mapCoder) Decode(dec *coders.Decoder) fieldValue {
	n := dec.Uint32()
	m := make(map[fieldValue]fieldValue, n)
	for range n {
		k := c.key.Decode(dec)
		v := c.value.Decode(dec)
		m[k] = v
	}
	return valueOfIface(m)
}

func (c *mapCoder) Encode(enc *coders.Encoder, val fieldValue) {
	m := val.getIface().(map[fieldValue]fieldValue)
	enc.Int32(int32(len(m)))
	for k, v := range m {
		c.key.Encode(enc, k)
		c.value.Encode(enc, v)
	}
}

// rowCoder is a `coders.Coder[fieldValue]` that produces a RowValue wrapped in an interface.
type rowCoder struct {
	namesToNums map[string]int // precomputed index.
	fields      []coders.Coder[fieldValue]
	nullable    []bool
}

var _ coders.Coder[fieldValue] = (*rowCoder)(nil)

// Decode implements coders.Coder.
func (c *rowCoder) Decode(dec *coders.Decoder) fieldValue {
	// Varint for the Number of Row field in this encoded row.
	nf := dec.Varint()
	// Varint for the length of the packed bit field for nils.
	// This is just a normal Byte buffer decoder.
	nils := bytes.Clone(dec.Bytes())

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
		row.fields[i] = fc.Decode(dec)
	}
	return valueOfIface(row)
}

// Encode implements coders.Coder.
func (c *rowCoder) Encode(enc *coders.Encoder, v fieldValue) {
	row := v.getIface().(RowValue)
	// Row header has the number of fields
	enc.Varint(uint64(len(row.fields)))
	// Followed by the nil bits.
	enc.Bytes(row.nils)

	for i, fc := range c.fields {
		if c.nullable[i] && isFieldNil(row.nils, i) {
			continue
		}
		fc.Encode(enc, row.fields[i])
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

// Logical Coders

// int64Coder is a fixed sized bigEndian representation of an int64.
type int64Coder struct{}

// Decode implements coders.Coder.
func (c *int64Coder) Decode(dec *coders.Decoder) fieldValue {
	return fieldValue{typ: int64Type, num: uint64(dec.Int64())}
}

// Encode implements coders.Coder.
func (c *int64Coder) Encode(enc *coders.Encoder, v fieldValue) {
	enc.Int64(int64(v.num))
}

// decimal is a temporary measure for handling the decimal
// logical type WRT to parsing them.
// Improve logical type handling will be some other time.
type decimal struct {
	Scale    int64
	TwosComp []byte
}

type decimalCoder struct{}

func (c *decimalCoder) Decode(dec *coders.Decoder) fieldValue {
	scale := int64(dec.Varint())
	data := dec.Bytes()
	return valueOfIface(decimal{Scale: scale, TwosComp: data})
}

func (c *decimalCoder) Encode(enc *coders.Encoder, val fieldValue) {
	deci := val.getIface().(decimal)
	enc.Varint(uint64(deci.Scale))
	enc.Bytes(deci.TwosComp)
}
