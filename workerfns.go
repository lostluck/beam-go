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

// workerfns.go is where SDK side transforms and their abstract graph representations live.
// They provide common utility that pipeline execution needs to correctly implement the beam model.
// Note that they are largely implemented in the same manner as user DoFns.

// multiplex and discard tranforms have no explicit edge, and are implicitly added to
// the execution graph when a PCollection is consumed by more than one transform, and zero
// transforms respectively.

// multiplex is a Transform inserted when a PCollection is used as an input into
// multiple downstream Transforms. The same element is emitted to each
// consuming emitter in order.
type multiplex[E Element] struct {
	Outs []PCol[E]
}

func (fn *multiplex[E]) ProcessBundle(dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		for _, out := range fn.Outs {
			out.Emit(ec, elm)
		}
		return nil
	})
	return nil
}

// discard is a Transform inserted when a PCollection is unused by a downstream Transform.
// It performs a no-op. This allows execution graphs to avoid branches and checks whether
// a consumer is valid on each element.
type discard[E Element] struct{}

func (fn *discard[E]) ProcessBundle(dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		return nil
	})
	return nil
}
