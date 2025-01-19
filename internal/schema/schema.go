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
	nils      []byte

	nameToNum map[string]int
	fields    []fieldValue
}

// Sets the value for the given field, and updates the nil bit.
func (v *RowValue) Set(fieldname string, val any) {
	v.fields[v.nameToNum[fieldname]].Val = val
}

// Sets the bit at pos in the byte b.
func setBit(b byte, pos uint) byte {
	b |= (1 << pos)
	return b
}

// Clears the bit at pos in b.
func clearBit(b byte, pos uint) byte {
	mask := byte(^(1 << pos))
	b &= mask
	return b
}

type fieldValue struct {
	Val any
}

// ToCoder takes a schema a produces a coder for RowValues of that schema.
func ToCoder(schema *pipepb.Schema) coders.Coder[RowValue] {
	if schema.GetEncodingPositionsSet() {
		panic("changed encoding positions not yet supported")
	}
	return &RowValueCoder{
		schema: proto.Clone(schema).(*pipepb.Schema),
	}
}

// RowValueCoder is a stateless coder for a provided schema to dynamic RowValues.
type RowValueCoder struct {
	schema *pipepb.Schema
}

var _ coders.Coder[RowValue] = (*RowValueCoder)(nil)

// isFieldNil examines the passed in packed bits nils buffer
// and returns true if the field at that index wasn't encoded
// and can be skipped in decoding.
func isFieldNil(nils []byte, f int) bool {
	i, b := f/8, f%8
	// https://github.com/apache/beam/issues/21232: The row header can elide trailing 0 bytes,
	// and we shouldn't care if there are trailing 0 bytes when doing a lookup.
	return i < len(nils) && len(nils) != 0 && (nils[i]>>uint8(b))&0x1 == 1
}

// Decode will decode bytes into a RowValue
func (c *RowValueCoder) Decode(dec *coders.Decoder) RowValue {
	return *decodeSchema(dec, c.schema)
}

func decodeField(dec *coders.Decoder, ft *pipepb.FieldType) any {
	switch ti := ft.GetTypeInfo().(type) {
	case *pipepb.FieldType_AtomicType:
		return decodeAtomic(dec, ti.AtomicType)
	case *pipepb.FieldType_ArrayType:
		return decodeArray(dec, ti.ArrayType.GetElementType())
	case *pipepb.FieldType_IterableType:
		return decodeArray(dec, ti.IterableType.GetElementType())
	case *pipepb.FieldType_MapType:
		return decodeMap(dec, ti.MapType)
	case *pipepb.FieldType_LogicalType:
		return decodeLogical(dec, ti.LogicalType)
	case *pipepb.FieldType_RowType:
		v := decodeSchema(dec, ti.RowType.GetSchema())
		return v
	default:
		panic(fmt.Sprintf("unimplemented field type: %v", prototext.Format(ft)))
	}
}

func decodeSchema(dec *coders.Decoder, schema *pipepb.Schema) *RowValue {
	// Varint for the Number of Row field in this encoded row.
	nf := dec.Varint()
	// Varint for the length of the packed bit field for nils.
	// This is just a normal Byte buffer decoder.
	nils := dec.Bytes()

	row := &RowValue{
		numFields: nf,
		nils:      nils,
		nameToNum: make(map[string]int),
	}
	for i, f := range schema.GetFields() {
		row.nameToNum[f.GetName()] = i
		row.fields = append(row.fields, fieldValue{})
		ft := f.GetType()
		if ft.GetNullable() && isFieldNil(nils, i) {
			continue
		}
		v := decodeField(dec, ft)
		row.fields[i].Val = v
	}
	return row
}

type decimal struct {
	Scale    int64
	TwosComp []byte
}

func decodeLogical(dec *coders.Decoder, logtype *pipepb.LogicalType) any {
	urn := logtype.GetUrn()
	switch urn {
	case "beam:logical_type:millis_instant:v1":
		// Non-conforming Representation.
		// Claims to be INT64, but uses a raw big endian encoding.
		return dec.Int64()
	case "beam:logical_type:decimal:v1":
		// Non-conforming Representation.
		// It claims "bytes", but is a varint of
		// the scale, followed by a followed by a bytes coding of the raw big
		// integer.
		// It's not a row structure either.
		scale := int64(dec.Varint())
		data := dec.Bytes()
		return decimal{Scale: scale, TwosComp: data}

	default:
		// Do nothing special, and just use the Representation raw.
		v := decodeField(dec, logtype.GetRepresentation())
		return v
	}
}

func decodeMap(dec *coders.Decoder, maptype *pipepb.MapType) any {
	// Map Decoding.
	n := dec.Uint32()

	m := make(map[any]any, n)
	for range n {
		k := decodeField(dec, maptype.GetKeyType())
		vt := maptype.GetValueType()
		if vt.GetNullable() {
			if dec.Byte() == 0x00 {
				m[k] = nil
				continue
			}
		}
		v := decodeField(dec, vt)
		m[k] = v
	}
	return m
}

func decodeArray(dec *coders.Decoder, ftype *pipepb.FieldType) any {
	// Iterable Decoding.
	n := dec.Uint32()
	arr := make([]any, 0, n)
	for range n {
		if ftype.GetNullable() {
			if dec.Byte() == 0x00 {
				arr = append(arr, nil)
				continue
			}
		}
		arr = append(arr, decodeField(dec, ftype))
	}
	return arr
}

func decodeAtomic(dec *coders.Decoder, atype pipepb.AtomicType) any {
	switch atype {
	case pipepb.AtomicType_BOOLEAN:
		return dec.Bool()
	case pipepb.AtomicType_BYTE:
		return dec.Byte()
	case pipepb.AtomicType_BYTES:
		return dec.Bytes()
	case pipepb.AtomicType_DOUBLE:
		return dec.Double()
	case pipepb.AtomicType_FLOAT:
		return dec.Float()

	// Ints are varint encoded
	case pipepb.AtomicType_INT16:
		return int16(dec.Varint())
	case pipepb.AtomicType_INT32:
		return int32(dec.Varint())
	case pipepb.AtomicType_INT64:
		return int64(dec.Varint())

	case pipepb.AtomicType_STRING:
		return dec.StringUtf8()
	default:
		panic(fmt.Sprintf("unimplemented atomic field type: %v", atype))
	}
}

// Encode implements coders.Coder.
func (c *RowValueCoder) Encode(enc *coders.Encoder, row RowValue) {
	encodeSchema(enc, c.schema, &row)
}

func encodeSchema(enc *coders.Encoder, schema *pipepb.Schema, row *RowValue) {
	// Row header has the number of fields
	enc.Varint(uint64(len(row.fields)))
	// Followed by the nil bits.
	enc.Bytes(row.nils)
	// Followed by the fields in encoding order.

	for i, f := range schema.GetFields() {
		ft := f.GetType()
		if ft.GetNullable() && isFieldNil(row.nils, i) {
			continue
		}
		encodeField(enc, ft, row.fields[i].Val)
	}
}

func encodeField(enc *coders.Encoder, ft *pipepb.FieldType, val any) {
	switch ti := ft.GetTypeInfo().(type) {
	case *pipepb.FieldType_AtomicType:
		encodeAtomic(enc, ti.AtomicType, val)
	case *pipepb.FieldType_ArrayType:
		encodeArray(enc, ti.ArrayType.GetElementType(), val)
	case *pipepb.FieldType_IterableType:
		encodeArray(enc, ti.IterableType.GetElementType(), val)
	case *pipepb.FieldType_MapType:
		encodeMap(enc, ti.MapType, val)
	case *pipepb.FieldType_LogicalType:
		encodeLogical(enc, ti.LogicalType, val)
	case *pipepb.FieldType_RowType:
		encodeSchema(enc, ti.RowType.GetSchema(), val.(*RowValue))
	default:
		panic(fmt.Sprintf("Encode: unimplemented field type: %v", prototext.Format(ft)))
	}
}

func encodeLogical(enc *coders.Encoder, logtype *pipepb.LogicalType, val any) {
	urn := logtype.GetUrn()
	switch urn {
	case "beam:logical_type:millis_instant:v1":
		// Non-conforming Representation.
		// Claims to be INT64, but uses a raw big endian encoding for the int64
		// instead of the schema row VarInt.
		enc.Int64(val.(int64))
	case "beam:logical_type:decimal:v1":
		// Non-conforming Representation.
		// It claims "bytes", but is a varint of
		// the scale, followed by a followed by a bytes coding of the raw big
		// integer.
		// It's not a row structure either.
		deci := val.(decimal)
		enc.Varint(uint64(deci.Scale))
		enc.Bytes(deci.TwosComp)
	default:
		// Do nothing special, and just use the Representation raw.
		encodeField(enc, logtype.GetRepresentation(), val)
	}
}

func encodeMap(enc *coders.Encoder, maptype *pipepb.MapType, val any) {
	// Map Decoding.
	m := val.(map[any]any)
	enc.Int32(int32(len(m)))
	for k, v := range m {
		encodeField(enc, maptype.GetKeyType(), k)

		vt := maptype.GetValueType()
		if vt.GetNullable() {
			if v == nil {
				enc.Byte(0x00)
				continue
			}
			enc.Byte(0x01)
		}

		encodeField(enc, vt, v)
	}
}

func encodeArray(enc *coders.Encoder, ftype *pipepb.FieldType, val any) {
	arr := val.([]any)
	enc.Int32(int32(len(arr)))
	for _, v := range arr {

		if ftype.GetNullable() {
			if v == nil {
				enc.Byte(0x00)
				continue
			}
			enc.Byte(0x01)
		}
		encodeField(enc, ftype, v)
	}
}

func encodeAtomic(enc *coders.Encoder, atype pipepb.AtomicType, val any) {
	switch atype {
	case pipepb.AtomicType_BOOLEAN:
		enc.Bool(val.(bool))
	case pipepb.AtomicType_BYTE:
		enc.Byte(val.(byte))
	case pipepb.AtomicType_BYTES:
		enc.Bytes(val.([]byte))
	case pipepb.AtomicType_DOUBLE:
		enc.Double(val.(float64))
	case pipepb.AtomicType_FLOAT:
		enc.Float(val.(float32))

	// Ints are Varint encoded.
	case pipepb.AtomicType_INT16:
		enc.Varint(uint64(val.(int16)))
	case pipepb.AtomicType_INT32:
		enc.Varint(uint64(val.(int32)))
	case pipepb.AtomicType_INT64:
		enc.Varint(uint64(val.(int64)))

	case pipepb.AtomicType_STRING:
		enc.StringUtf8(val.(string))
	default:
		panic(fmt.Sprintf("unimplemented atomic field type: %v", atype))
	}
}
