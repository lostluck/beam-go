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

import (
	"log/slog"
	"testing"
	"testing/slogtest"

	"google.golang.org/protobuf/types/known/structpb"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
)

func TestSlogtest(t *testing.T) {
	out := make(chan *fnpb.LogEntry, 100)
	slogtest.Run(t,
		func(_ *testing.T) slog.Handler { return newLoggingHandler(out, nil) },
		func(_ *testing.T) map[string]any {
			return parseLogEntries(<-out)
		})
}

func parseLogEntries(data *fnpb.LogEntry) map[string]any {
	m := map[string]any{
		slog.MessageKey: data.Message,
	}
	if data.Timestamp != nil {
		m[slog.TimeKey] = data.Timestamp.AsTime()
	}
	switch data.Severity {
	case fnpb.LogEntry_Severity_INFO:
		m[slog.LevelKey] = slog.LevelInfo
	}
	if data.LogLocation != "" {
		m[slog.SourceKey] = data.LogLocation
	}
	for k, v := range structToMap(data.CustomData) {
		m[k] = v
	}
	return m
}

func structToMap(s *structpb.Struct) map[string]any {
	m := map[string]any{}
	for k, v := range s.GetFields() {
		switch v.Kind.(type) {
		case *structpb.Value_StructValue:
			m[k] = structToMap(v.GetStructValue())
		default:
			m[k] = v.AsInterface()
		}
	}
	return m
}

func TestWithTransformID(t *testing.T) {
	out := make(chan *fnpb.LogEntry, 100)
	want := handlerOptions{
		InstID: "testInstruction",
	}

	l := slog.New(newLoggingHandler(out, &want))
	l.Info("testMsg1")

	got := <-out
	if got.InstructionId != string(want.InstID) {
		t.Errorf("logging handler didn't set InstructionID, got %q want %q", got.InstructionId, want.InstID)
	}
	if got.TransformId != want.TransformId {
		t.Errorf("logging handler didn't set TransformId, got %q want %q", got.TransformId, want.TransformId)
	}

	want.TransformId = "testTransformID"
	l2 := l.With(withTransformID(want.TransformId))

	l2.Info("testMsg2")

	got = <-out
	if got.InstructionId != string(want.InstID) {
		t.Errorf("logging handler didn't set InstructionID, got %q want %q", got.InstructionId, want.InstID)
	}
	if got.TransformId != want.TransformId {
		t.Errorf("logging handler didn't set TransformId, got %q want %q", got.TransformId, want.TransformId)
	}

	// The original logger should still have an unset transform id.
	l.Warn("testMsg1")
	got = <-out
	if got.TransformId != "" {
		t.Errorf("initial logging handler is aliasing TransformId, got %q want %q", got.TransformId, "")
	}
}
