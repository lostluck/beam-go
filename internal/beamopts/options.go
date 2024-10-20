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

package beamopts

import "lostluck.dev/beam-go/internal"

// Options is the common options type shared across beam packages.
type Options interface {
	// JSONOptions is exported so related beam packages can implement Options.
	BeamOptions(internal.NotForPublicUse)
}

// Struct is the combination of all options in struct form.
// This is efficient to pass down the call stack and to query.
type Struct struct {
	Name     string // The configured name of the options target. Otherwise it's autogeneerated.
	Endpoint string // The configured URL for a service.
}

func (dst *Struct) BeamOptions(internal.NotForPublicUse) {}

func (dst *Struct) Join(srcs ...Options) {
	for _, src := range srcs {
		switch src := src.(type) {
		case *Struct:
			if src.Name != "" {
				dst.Name = src.Name
			}
			if src.Endpoint != "" {
				dst.Endpoint = src.Endpoint
			}
		}
	}
}
