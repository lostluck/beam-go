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

// Package beam is a version of an Apache Beam SDK for Go that
// leverages generics. It's currently aimed at exploring the approach for
// the purposes of Data processing.
//
// # Status: Prototype
//
// This is published to receive feedback and similar on the approach, and
// design.
//
// This package serves three purposes:
//
//   - implementing DoFns
//   - Constructing Pipelines.
//   - Launching and managing Jobs.
//
// # Implementing DoFns
//
// DoFns define execution time behavior for the pipeline, processing, and
// transforming elements.
//
// DoFns are primarily intended to be pointers to a struct type that implement
// the [Transform] generic interface, using the [DFC] type.
//
// Further, the [ElmC] type is used within [Process] functions for Per Element
// use.
//
// TODO(lostluck): Elaborate on how to build a DoFn.
//
// # Constructing Pipelines
//
// Pipelines are constructed and launched by loading construction functions into
// a [Configuration], calling [Configuration.Ready] to receive a [Launcher], and
// then [Launcher.Run] to start pipeline execution. This returns a [Pipeline]
// handle that can be used to block until the pipeline is complete, or query
// active pipeline status and metrics.
//
// Simple single pipelines can use the [Launch] or [LaunchAndWait] convenience
//
// TODO(lostluck): Elaborate on how to build a pipeline.
//
// # Running Jobs
//
//   - TODO Cover launching a binary
//   - TODO Cover testing and Metrics
//   - TODO Cover flags
//
// # General classes of functions and types
//
//   - 'As' prefixed function convert values into initialized distributable types. Used for Combiners, SideInputs, and some States.
//   - (Considering)`Then` prefixed functions are for pipeline construction.
package beam
