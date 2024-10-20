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

package harness

import "log/slog"

// Port represents the connection port of external operations.
type Port struct {
	URL string
}

// StreamID represents the static information needed to identify
// a data stream. Dynamic information, notably bundleID, is provided
// implicitly by the managers.
type StreamID struct {
	Port         Port
	PtransformID string
}

// Elements holds data or timers sent across the data channel.
// If TimerFamilyID is populated, it's a timer, otherwise it's
// data elements.
type Elements struct {
	Data, Timers                []byte
	TimerFamilyID, PtransformID string
}

// DataContext holds connectors to various data connections, incl. state and side input.
type DataContext struct {
	Data  *ScopedDataManager
	State *ScopedStateManager

	logger *slog.Logger
	bdID   bundleDescriptorID
	instID instructionID
}

// LoggerForTransform produces a logger for transform with transformID.
// The ID must be sourced from a ProcessBundleDescriptor so messages
// can be matched up with their respective transform.
func (dc *DataContext) LoggerForTransform(transformID string) *slog.Logger {
	return dc.logger.With(withTransformID(transformID))
}
