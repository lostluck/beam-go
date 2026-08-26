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

import (
	"testing"

	"lostluck.dev/beam-go/internal"
)

func TestStruct_Join(t *testing.T) {
	tests := []struct {
		name         string
		initial      Struct
		sources      []Options
		wantName     string
		wantEndpoint string
	}{
		{
			name:         "empty_initial_joined_with_options",
			initial:      Struct{},
			sources:      []Options{&Struct{Name: "job1"}, &Struct{Endpoint: "localhost:8080"}},
			wantName:     "job1",
			wantEndpoint: "localhost:8080",
		},
		{
			name:         "overwrite_existing_name",
			initial:      Struct{Name: "old-name", Endpoint: "localhost:5000"},
			sources:      []Options{&Struct{Name: "new-name"}},
			wantName:     "new-name",
			wantEndpoint: "localhost:5000",
		},
		{
			name:         "ignore_empty_in_source",
			initial:      Struct{Name: "keep-me", Endpoint: "localhost:5000"},
			sources:      []Options{&Struct{}},
			wantName:     "keep-me",
			wantEndpoint: "localhost:5000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst := tc.initial
			dst.BeamOptions(internal.NotForPublicUse{})
			dst.Join(tc.sources...)

			if dst.Name != tc.wantName {
				t.Errorf("Name = %q, want %q", dst.Name, tc.wantName)
			}
			if dst.Endpoint != tc.wantEndpoint {
				t.Errorf("Endpoint = %q, want %q", dst.Endpoint, tc.wantEndpoint)
			}
		})
	}
}
