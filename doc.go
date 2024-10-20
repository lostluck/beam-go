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

// Package beam is an experimental version of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// Notably, it's using a completely different approach to building and executing
// the pipeline, in order to allow Go to typecheck the pipeline, instead of
// reflection heavy SDK side code.
//
// Things that are different from the original Apache Beam Go SDK.
// - Coders
// - DoFns
// - Pipeline Construction
// - No registration required.
// - Re-builds pipelines on worker, along with managing that construction time.
package beam
