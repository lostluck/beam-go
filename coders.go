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

package beam

import (
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
	"lostluck.dev/beam-go/window"
)

// coderFromProto bridges the gap between the Go type, and the
// proto coder. This is necessary since sometimes a runner may adjust
// a coder, such as adding length prefixes.
func coderFromProto[E any](cs map[string]*pipepb.Coder, cid string) coders.Coder[E] {
	c, ok := cs[cid]
	if ok {
		switch c.GetSpec().GetUrn() {
		case "beam:coder:length_prefix:v1":
			ccid := c.GetComponentCoderIds()[0]
			return &lpCoder[E]{Coder: coderFromProto[E](cs, ccid)}
		case "beam:coder:windowed_value:v1":
			// Doesn't happen often, but generally from sources and sinks.
			ccid := c.GetComponentCoderIds()[0]
			return coderFromProto[E](cs, ccid)
		case "beam:coder:kv:v1",
			"beam:coder:state_backed_iterable:v1",
			"beam:coder:iterable:v1":
			// Handled by the structured coder clause below.
		}
	}
	var e E
	a := any(e)
	switch a := a.(type) {
	case structuredCoder:
		return a.makeCoder(cs, cid).(coders.Coder[E])
	}
	// Just infer primitives directly.
	return coders.MakeCoder[E]()
}

func MakeCoder[E any]() coders.Coder[E] {
	var e E
	a := any(e)
	switch a := a.(type) {
	case structuredCoder:
		return a.makeCoder(nil, "").(coders.Coder[E])
	}
	return coders.MakeCoder[E]()
}

type structuredCoder interface {
	makeCoder(cs map[string]*pipepb.Coder, cid string) any
}

var _ structuredCoder = KV[int, int]{}

func (KV[K, V]) makeCoder(cs map[string]*pipepb.Coder, cid string) any {
	c := cs[cid]
	var kcid, vcid string
	if c != nil && len(c.GetComponentCoderIds()) >= 2 {
		kcid = c.GetComponentCoderIds()[0]
		vcid = c.GetComponentCoderIds()[1]
	}
	return kvCoder[K, V]{
		KCoder: coderFromProto[K](cs, kcid),
		VCoder: coderFromProto[V](cs, vcid),
	}
}

type kvCoder[K, V Element] struct {
	KCoder coders.Coder[K]
	VCoder coders.Coder[V]
}

func (c kvCoder[K, V]) Encode(enc *coders.Encoder, v KV[K, V]) {
	c.KCoder.Encode(enc, v.Key)
	c.VCoder.Encode(enc, v.Value)
}

func (c kvCoder[K, V]) Decode(dec *coders.Decoder) KV[K, V] {
	return KV[K, V]{
		Key:   c.KCoder.Decode(dec),
		Value: c.VCoder.Decode(dec),
	}
}

var _ structuredCoder = Iter[int]{}

func (Iter[V]) makeCoder(cs map[string]*pipepb.Coder, cid string) any {
	c := cs[cid]
	var vcid string
	if c != nil && len(c.GetComponentCoderIds()) >= 1 {
		vcid = c.GetComponentCoderIds()[0]
	}
	return iterCoder[V]{
		VCoder: coderFromProto[V](cs, vcid),
	}
}

type iterCoder[V Element] struct {
	VCoder coders.Coder[V]
}

func (c iterCoder[V]) Encode(enc *coders.Encoder, v Iter[V]) {
	panic("iterators are unencodeable")
}

func (c iterCoder[V]) Decode(dec *coders.Decoder) Iter[V] {
	n := dec.Int32()
	var cur int32
	return Iter[V]{
		source: func() (V, bool) {
			if cur >= n {
				var dummy V
				return dummy, false
			}
			cur++
			return c.VCoder.Decode(dec), true
		},
	}
}

// lpCoder takes a different coder for a type, and deals with
// length prefixes, adding them on encoding, reading them on
// decode.
type lpCoder[E Element] struct {
	Coder coders.Coder[E]
}

func (c *lpCoder[E]) Encode(enc *coders.Encoder, v E) {
	inner := coders.NewEncoder()
	c.Coder.Encode(inner, v)
	// Use the bytes encoding, since it's already a length prefix
	// followed by that many bytes.
	enc.Bytes(inner.Data())
}

func (c *lpCoder[E]) Decode(dec *coders.Decoder) E {
	// Use the bytes decoding, since it's already a length prefix
	// followed by that many bytes.
	inner := coders.NewDecoder(dec.Bytes())
	return c.Coder.Decode(inner)
}

type windowCoder interface {
	Encode(enc *coders.Encoder, w window.BoundedWindow)
	Decode(dec *coders.Decoder) window.BoundedWindow
}

type globalWindowCoderWrapper struct{}

func (globalWindowCoderWrapper) Encode(enc *coders.Encoder, w window.BoundedWindow) {
	enc.GlobalWindow()
}

func (globalWindowCoderWrapper) Decode(dec *coders.Decoder) window.BoundedWindow {
	dec.GlobalWindow()
	return window.GlobalWindow{}
}

type intervalWindowCoderWrapper struct{}

func (intervalWindowCoderWrapper) Encode(enc *coders.Encoder, w window.BoundedWindow) {
	if iw, ok := w.(window.IntervalWindow); ok {
		enc.IntervalWindow(iw.End, iw.Duration())
	} else {
		enc.GlobalWindow()
	}
}

func (intervalWindowCoderWrapper) Decode(dec *coders.Decoder) window.BoundedWindow {
	end, dur := dec.IntervalWindow()
	return window.IntervalWindow{
		Start: end.Add(-dur),
		End:   end,
	}
}

func windowCoderFromProto(cs map[string]*pipepb.Coder, cid string) windowCoder {
	c, ok := cs[cid]
	if ok {
		switch c.GetSpec().GetUrn() {
		case "beam:coder:length_prefix:v1":
			if len(c.GetComponentCoderIds()) > 0 {
				return windowCoderFromProto(cs, c.GetComponentCoderIds()[0])
			}
		case "beam:coder:global_window:v1":
			return globalWindowCoderWrapper{}
		case "beam:coder:interval_window:v1":
			return intervalWindowCoderWrapper{}
		}
	}
	return globalWindowCoderWrapper{}
}

func extractWindowCoderFromWV(cs map[string]*pipepb.Coder, cid string) windowCoder {
	c, ok := cs[cid]
	if ok {
		switch c.GetSpec().GetUrn() {
		case "beam:coder:length_prefix:v1":
			if len(c.GetComponentCoderIds()) > 0 {
				return extractWindowCoderFromWV(cs, c.GetComponentCoderIds()[0])
			}
		case "beam:coder:windowed_value:v1":
			if len(c.GetComponentCoderIds()) > 1 {
				return windowCoderFromProto(cs, c.GetComponentCoderIds()[1])
			}
		}
	}
	return globalWindowCoderWrapper{}
}
