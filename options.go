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

import "lostluck.dev/beam-go/internal/beamopts"

// Options configure Run, ParDo, and Combine with specific features.
// Each function takes a variadic list of options, where properties
// set in later options override the value of previously set properties.
type Options = beamopts.Options

// Name sets the name of the pipeline or transform in question, typically
// to make it easier to refer to.
func Name(name string) Options {
	return &beamopts.Struct{
		Name: name,
	}
}

// Endpoint sets the url when applicable, such as the JobManagement endpoint for submitting jobs
// or for configuring a target for expansion services.
func Endpoint(endpoint string) Options {
	return &beamopts.Struct{
		Endpoint: endpoint,
	}
}
