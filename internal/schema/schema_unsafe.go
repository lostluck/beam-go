// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"math"
	"unsafe"
)

type (
	ifaceHeader struct {
		_    [0]any // if interfaces have greater alignment than unsafe.Pointer, this will enforce it.
		Type unsafe.Pointer
		Data unsafe.Pointer
	}
)

var (
	nilType      = typeOf(nil)
	boolType     = typeOf(*new(bool))
	intType      = typeOf(*new(int))
	int8Type     = typeOf(*new(int8))
	int16Type    = typeOf(*new(int16))
	int32Type    = typeOf(*new(int32))
	int64Type    = typeOf(*new(int64))
	uintType     = typeOf(*new(uint))
	uint8Type    = typeOf(*new(uint8))
	uint16Type   = typeOf(*new(uint16))
	uint32Type   = typeOf(*new(uint32))
	uint64Type   = typeOf(*new(uint64))
	float32Type  = typeOf(*new(float32))
	float64Type  = typeOf(*new(float64))
	stringType   = typeOf(*new(string))
	bytesType    = typeOf(*new([]byte))
	rowValueType = typeOf(*new(RowValue))
)

// typeOf returns a pointer to the Go type information.
// The pointer is comparable and equal if and only if the types are identical.
func typeOf(t any) unsafe.Pointer {
	return (*ifaceHeader)(unsafe.Pointer(&t)).Type
}

// value is a union where only one type can be represented at a time.
// The struct is 24B large on 64-bit systems and requires the minimum storage
// necessary to represent each possible type.
//
// The Go GC needs to be able to scan variables containing pointers.
// As such, pointers and non-pointers cannot be intermixed.
type fieldValue struct {
	// typ stores the type of the value as a pointer to the Go type.
	typ unsafe.Pointer // 8B

	// ptr stores the data pointer for a String, Bytes, or interface value.
	ptr unsafe.Pointer // 8B

	// num stores numbers as a raw uint64.
	//
	// It is also used to store the length of a String or Bytes value;
	// the capacity is ignored.
	num uint64 // 8B
}

func valueOfString(v string) fieldValue {
	return fieldValue{typ: stringType, ptr: unsafe.Pointer(unsafe.StringData(v)), num: uint64(len(v))}
}
func valueOfBytes(v []byte) fieldValue {
	return fieldValue{typ: bytesType, ptr: unsafe.Pointer(unsafe.SliceData(v)), num: uint64(len(v))}
}
func valueOfIface(v any) fieldValue {
	p := (*ifaceHeader)(unsafe.Pointer(&v))
	return fieldValue{typ: p.Type, ptr: p.Data}
}

func (v fieldValue) getString() string {
	return unsafe.String((*byte)(v.ptr), v.num)
}
func (v fieldValue) getBytes() []byte {
	return unsafe.Slice((*byte)(v.ptr), v.num)
}
func (v fieldValue) getIface() (x any) {
	*(*ifaceHeader)(unsafe.Pointer(&x)) = ifaceHeader{Type: v.typ, Data: v.ptr}
	return x
}

// Interface returns v as an any.
//
// Invariant: v == ValueOf(v).Interface()
func (v fieldValue) Interface() any {
	switch v.typ {
	case nilType:
		return nil
	case boolType:
		return v.num > 0
	case int32Type:
		return int32(v.num)
	case int64Type:
		return int64(v.num)
	case uint32Type:
		return uint32(v.num)
	case uint64Type:
		return uint64(v.num)
	case float32Type:
		return float32(math.Float64frombits(uint64(v.num)))
	case float64Type:
		return float64(math.Float64frombits(uint64(v.num)))
	case stringType:
		return v.getString()
	case bytesType:
		return v.getBytes()
	default:
		return v.getIface()
	}
}

func valFromIface(v any) fieldValue {
	switch val := v.(type) {
	case nil:
		return fieldValue{typ: nilType}
	case bool:
		if val {
			return fieldValue{typ: boolType, num: 1}
		}
		return fieldValue{typ: boolType, num: 0}
	case int32:
		return fieldValue{typ: int32Type, num: uint64(val)}
	case int64:
		return fieldValue{typ: int64Type, num: uint64(val)}
	case uint32:
		return fieldValue{typ: uint32Type, num: uint64(val)}
	case uint64:
		return fieldValue{typ: uint64Type, num: val}
	case float32:
		return fieldValue{typ: float32Type, num: math.Float64bits(float64(val))}
	case float64:
		return fieldValue{typ: float64Type, num: math.Float64bits(float64(val))}
	case string:
		return valueOfString(val)
	case []byte:
		return valueOfBytes(val)
	default:
		return valFromIface(v)
	}
}
