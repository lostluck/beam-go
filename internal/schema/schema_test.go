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

package schema

import (
	"bytes"
	_ "embed"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/text/encoding/charmap"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v2"
	"lostluck.dev/beam-go/coders"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

var (
	atomT = func(nullable bool, typ pipepb.AtomicType) *pipepb.FieldType {
		return &pipepb.FieldType{
			Nullable: nullable,
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: typ,
			},
		}
	}
	strField       = atomT(false, pipepb.AtomicType_STRING)
	nillableString = atomT(true, pipepb.AtomicType_STRING)

	suite = []struct {
		name   string
		schema *pipepb.Schema
		data   []byte
	}{
		{
			name:   "empty",
			schema: &pipepb.Schema{},
			data:   []byte{0, 0},
		}, {
			name: "oneString",
			schema: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{Name: "first", Type: strField},
				},
			},
			data: []byte{1, 0, 1, 'a'},
		}, {
			name: "oneNillableString_nil",
			schema: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{Name: "first", Type: nillableString},
				},
			},
			data: []byte{1, 1, 1},
		}, {
			name: "oneNillableString_val",
			schema: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{Name: "first", Type: nillableString},
				},
			},
			data: []byte{1, 0, 2, 'a', 'b'},
		}, {
			name: "variousNillableStrings",
			schema: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{Name: "first", Type: nillableString},
					{Name: "second", Type: nillableString},
					{Name: "third", Type: nillableString},
				},
			},
			data: []byte{3, 1, 2, 2, 'a', 'b', 4, 'b', 'e', 'a', 'm'},
		}, {
			name: "various",
			schema: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{Name: "boolean", Type: atomT(false, pipepb.AtomicType_BOOLEAN)},
					{Name: "byte", Type: atomT(false, pipepb.AtomicType_BYTE)},
					{Name: "bytes", Type: atomT(false, pipepb.AtomicType_BYTES)},
					{Name: "double", Type: atomT(false, pipepb.AtomicType_DOUBLE)},
					{Name: "float", Type: atomT(false, pipepb.AtomicType_FLOAT)},
					{Name: "short", Type: atomT(false, pipepb.AtomicType_INT16)},
					{Name: "int", Type: atomT(false, pipepb.AtomicType_INT32)},
					{Name: "long", Type: atomT(false, pipepb.AtomicType_INT64)},
					{Name: "string", Type: atomT(false, pipepb.AtomicType_STRING)},
				},
			},
			data: []byte{9, // 9 fields
				0,           // no nulls
				1,           // true bool
				2,           // single byte
				2, '2', '2', // []byte
				'\u007f', '\u00ef', '\u00ff', '\u00ff', '\u00ff', '\u00ff', '\u00ff', '\u00ff', // double
				64, 73, 15, 208, // float
				'\u00a9', '\u0046', // int16
				'\u00a9', '\u0046', // int32
				'\u00a9', '\u0046', // int64
				4, 'b', 'e', 'a', 'm', // string
			},
		},
	}
)

func TestRowCoder(t *testing.T) {
	for _, test := range suite {
		t.Run(test.name, func(t *testing.T) {
			c := ToCoder(test.schema)

			r := coders.Decode(c, test.data)
			if got, want := coders.Encode(c, r), test.data; !cmp.Equal(got, want) {
				t.Errorf("round trip decode-encode not equal: want %v, got %v", want, got)
			}
		})
	}
}

func TestRowValue(t *testing.T) {
	test := suite[4]
	c := ToCoder(test.schema)

	t.Run("nilFields", func(t *testing.T) {
		row := coders.Decode(c, test.data)

		row.Set("first", nil)
		row.Set("second", nil)
		row.Set("third", nil)

		want := []byte{3, 1, 7}
		if got := coders.Encode(c, row); !cmp.Equal(got, want) {
			t.Errorf("encoding not equal: want %v, got %v", want, got)
		}
	})
	t.Run("setFields", func(t *testing.T) {
		row := coders.Decode(c, test.data)

		row.Set("first", "do")
		row.Set("second", "ray")
		row.Set("third", "mi")

		want := []byte{3, 1, 0, 2, 'd', 'o', 3, 'r', 'a', 'y', 2, 'm', 'i'}
		if got := coders.Encode(c, row); !cmp.Equal(got, want) {
			t.Errorf("encoding not equal: want %v, got %v", want, got)
		}
	})
	t.Run("setFields", func(t *testing.T) {
		row := coders.Decode(c, test.data)

		row.Set("first", nil)
		row.Set("second", "ray")
		row.Set("third", nil)

		want := []byte{3, 1, 5, 3, 'r', 'a', 'y'}
		if got := coders.Encode(c, row); !cmp.Equal(got, want) {
			t.Errorf("encoding not equal: want %v, got %v", want, got)
		}
	})

	t.Run("getFields", func(t *testing.T) {
		row := coders.Decode(c, test.data)

		if got, want := row.Get("first"), "ab"; got != want {
			t.Errorf("unexpected first field value: want %v, got %v", want, got)
		}
		if got, want := row.Get("second"), (any)(nil); got != want {
			t.Errorf("unexpected first field value: want %v, got %v", want, got)
		}
		if got, want := row.Get("third"), "beam"; got != want {
			t.Errorf("unexpected first field value: want %v, got %v", want, got)
		}
	})

}

// BenchmarkRoundtrip initial, unoptimized results.
//
// goos: linux
// goarch: amd64
// pkg: lostluck.dev/beam-go/internal/schema
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkRoundtrip/empty-16         	  307964	      3648 ns/op	    1720 B/op	      24 allocs/op
// BenchmarkRoundtrip/oneString-16     	  220701	      5174 ns/op	    1816 B/op	      28 allocs/op
// BenchmarkRoundtrip/oneNillableString_nil-16         	  250714	      4303 ns/op	    1800 B/op	      27 allocs/op
// BenchmarkRoundtrip/oneNillableString_val-16         	  182938	      6614 ns/op	    1952 B/op	      31 allocs/op
// BenchmarkRoundtrip/variousNillableStrings-16        	   98568	     11805 ns/op	    2312 B/op	      38 allocs/op
// BenchmarkRoundtrip/various-16                       	   44137	     26037 ns/op	    3072 B/op	      47 allocs/op
func BenchmarkRoundtrip(b *testing.B) {
	for _, test := range suite {
		b.Run(test.name, func(b *testing.B) {
			c := ToCoder(test.schema)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := coders.Decode(c, test.data)
				if got, want := coders.Encode(c, r), test.data; !cmp.Equal(got, want) {
					b.Errorf("round trip decode-encode not equal: want %v, got %v", want, got)
				}
			}
		})
	}
}

//go:embed standard_coders.yaml
var std_coders_yaml []byte

// spec is a set of conditions that a coder must pass.
type spec struct {
	Coder    *coder        `yaml:"coder,omitempty"`
	Nested   *bool         `yaml:"nested,omitempty"`
	Examples yaml.MapSlice `yaml:"examples,omitempty"`
}

type coder struct {
	Urn              string `yaml:"urn,omitempty"`
	Payload          string `yaml:"payload,omitempty"`
	NonDeterministic bool   `yaml:"non_deterministic,omitempty"`
}

func TestStandardRowCoders(t *testing.T) {
	specs := bytes.Split(std_coders_yaml, []byte("\n---\n"))
	for i, data := range specs {
		spec := spec{}
		if err := yaml.Unmarshal(data, &spec); err != nil {
			t.Errorf("unable to parse yaml: %v %q", err, data)
			continue
		}

		coder := spec.Coder
		if coder.Urn == "" {
			t.Fatalf("decode error, empty urn")
		}
		// Skip any non-row coders
		if coder.Urn != "beam:coder:row:v1" {
			continue
		}

		payload := coder.Payload

		schema := &pipepb.Schema{}
		if err := proto.Unmarshal([]byte(payload), schema); err != nil {
			t.Fatal(err)
		}
		if schema.GetEncodingPositionsSet() {
			continue
		}
		underTest := ToCoder(schema)

		for _, v := range spec.Examples {
			// Ignoring the JSON values for now, but using them as the name.
			// jsonValRaw :=[]byte(examples[k].(string))
			t.Run(fmt.Sprintf("row-%03d-%v", i, v.Value), func(t *testing.T) {
				// The bytes are no utf8 encoded, so we need to recode them to the right format.
				k, err := charmap.ISO8859_1.NewEncoder().String(v.Key.(string))
				defer func() {
					if e := recover(); e != nil {
						t.Fatal(k, []byte(k), e, string(debug.Stack()))
					}
				}()
				if err != nil {
					t.Fatalf("error recoding encoded bytes: %v", err)
				}
				r := coders.Decode(underTest, []byte(k))

				if got, want := coders.Encode(underTest, r), []byte(k); !cmp.Equal(got, want) {
					if coder.NonDeterministic {
						// We'll skip failing if round trips aren't equal for
						// Non deterministic coders.
						// This does ensure they don't panic at least.
						return
					}
					t.Errorf("round trip decode-encode not equal: want %v, got %v", want, got)
				}
			})
		}
	}
}
