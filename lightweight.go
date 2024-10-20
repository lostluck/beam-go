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
	"fmt"

	"lostluck.dev/beam-go/internal/beamopts"
)

type mapper[I, O Element] struct {
	fn  func(I) O
	Key string

	Output PCol[O]
}

func (fn *mapper[I, O]) ProcessBundle(dfc *DFC[I]) error {
	return dfc.Process(func(ec ElmC, in I) error {
		out := fn.fn(in)
		fn.Output.Emit(ec, out)
		return nil
	})
}

// lightweightInit is used by reconstruction.
func (fn *mapper[I, O]) lightweightInit(metadata map[string]any) {
	fn.fn = metadata[fn.Key].(func(I) O)
}

func Map[I, O Element](s *Scope, input PCol[I], lambda func(I) O, opts ...beamopts.Options) PCol[O] {
	ei := s.g.curEdgeIndex()
	// Store the transform in the metadata
	// with an index specific key.
	key := fmt.Sprintf("map%03d", ei)
	out := ParDo(s, input, &mapper[I, O]{fn: lambda, Key: key}, opts...)

	if s.g.edgeMeta == nil {
		s.g.edgeMeta = make(map[string]any)
	}
	s.g.edgeMeta[key] = lambda

	return out.Output
}

type lightweightIniter interface {
	lightweightInit(metadata map[string]any)
}
