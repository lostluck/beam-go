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
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
)

type fakeDataClientWithTimers struct {
	recv    chan *fnpb.Elements
	recvErr error
	recvMu  sync.Mutex

	send    chan *fnpb.Elements
	sendErr error
	sendMu  sync.Mutex
}

func (f *fakeDataClientWithTimers) Send(req *fnpb.Elements) error {
	f.sendMu.Lock()
	err := f.sendErr
	f.sendMu.Unlock()
	if err != nil {
		return err
	}
	f.send <- req
	return nil
}

func (f *fakeDataClientWithTimers) Recv() (*fnpb.Elements, error) {
	return nil, nil
}

func (f *fakeDataClientWithTimers) RecvMsg(m any) error {
	f.recvMu.Lock()
	err := f.recvErr
	f.recvMu.Unlock()
	if err != nil {
		return err
	}
	msg, ok := <-f.recv
	if !ok {
		return io.EOF
	}
	elem := m.(*fnpb.Elements)
	elem.Data = msg.Data
	elem.Timers = msg.Timers
	return nil
}

func (f *fakeDataClientWithTimers) setRecvErr(err error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	f.recvErr = err
}

func (f *fakeDataClientWithTimers) setSendErr(err error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	f.sendErr = err
}

func TestScopedDataManager_Lifecycle(t *testing.T) {
	ctx := t.Context()
	mgr := &DataChannelManager{
		ports: make(map[string]*DataChannel),
	}

	fakeClient := &fakeDataClientWithTimers{
		recv: make(chan *fnpb.Elements, 10),
		send: make(chan *fnpb.Elements, 10),
	}
	dc := makeDataChannel(ctx, "port-1", fakeClient, func() {})
	mgr.ports["port-1"] = dc

	scoped := NewScopedDataManager(mgr, "inst-1")

	// OpenWrite
	w, err := scoped.OpenWrite(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"})
	if err != nil {
		t.Fatalf("OpenWrite failed: %v", err)
	}
	go func() {
		<-fakeClient.send
	}()
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Errorf("Write error: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}

	// OpenTimerWrite
	tw, err := scoped.OpenTimerWrite(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"}, "family1")
	if err != nil {
		t.Fatalf("OpenTimerWrite failed: %v", err)
	}
	go func() {
		<-fakeClient.send
	}()
	if _, err := tw.Write([]byte("timer-data")); err != nil {
		t.Errorf("Timer write error: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Errorf("Timer close error: %v", err)
	}

	// OpenElementChan
	ch, err := scoped.OpenElementChan(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"}, nil)
	if err != nil {
		t.Fatalf("OpenElementChan failed: %v", err)
	}
	_ = ch

	// Close scoped manager
	if err := scoped.Close(); err != nil {
		t.Errorf("scoped.Close failed: %v", err)
	}

	// Operations after close should fail
	postCloseTests := []struct {
		name string
		op   func() error
	}{
		{
			name: "OpenWrite_after_close",
			op: func() error {
				_, err := scoped.OpenWrite(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"})
				return err
			},
		},
		{
			name: "OpenElementChan_after_close",
			op: func() error {
				_, err := scoped.OpenElementChan(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"}, nil)
				return err
			},
		},
		{
			name: "OpenTimerWrite_after_close",
			op: func() error {
				_, err := scoped.OpenTimerWrite(ctx, StreamID{Port: Port{URL: "port-1"}, PtransformID: "t1"}, "f")
				return err
			},
		},
	}

	for _, tc := range postCloseTests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.op(); err == nil {
				t.Errorf("%s succeeded, expected error after Close", tc.name)
			}
		})
	}
}

func TestDataChannelManager_PanicsAndCaching(t *testing.T) {
	mgr := &DataChannelManager{}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected Open with empty port to panic")
		}
	}()
	_, _ = mgr.Open(t.Context(), Port{URL: ""})
}

func TestDataChannel_ReadRecvMsg(t *testing.T) {
	ctx := t.Context()

	fakeClient := &fakeDataClientWithTimers{
		recv: make(chan *fnpb.Elements, 10),
		send: make(chan *fnpb.Elements, 10),
	}
	dc := makeDataChannel(ctx, "test-port", fakeClient, func() {})

	// Send message with Data, Timers, and TransformMonitoringInfos
	fakeClient.recv <- &fnpb.Elements{
		Data: []*fnpb.Elements_Data{
			{
				InstructionId: "inst-1",
				TransformId:   "t1",
				Data:          []byte("data-chunk"),
				IsLast:        true,
			},
		},
		Timers: []*fnpb.Elements_Timers{
			{
				InstructionId: "inst-1",
				TransformId:   "t1",
				TimerFamilyId: "tf1",
				Timers:        []byte("timer-chunk"),
				IsLast:        true,
			},
		},
	}

	ch, err := dc.OpenElementChan(ctx, "t1", "inst-1", nil)
	if err != nil {
		t.Fatalf("OpenElementChan failed: %v", err)
	}

	select {
	case elem, ok := <-ch:
		if !ok {
			t.Errorf("channel closed unexpectedly early")
		} else if len(elem.Data) == 0 && len(elem.Timers) == 0 {
			t.Errorf("empty element received: %+v", elem)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for elements")
	}

	// Trigger error on read
	fakeClient.setRecvErr(fmt.Errorf("read error"))
	fakeClient.recv <- &fnpb.Elements{}
	time.Sleep(50 * time.Millisecond)

	// Now OpenElementChan should fail with readErr
	if _, err := dc.OpenElementChan(ctx, "t2", "inst-2", nil); err == nil {
		t.Errorf("expected OpenElementChan to fail after readErr")
	}
}

func TestScopedStateManager_Lifecycle(t *testing.T) {
	ctx := t.Context()
	mgr := &StateChannelManager{
		ports: make(map[string]*StateChannel),
	}

	fakeClient := &fakeStateClient{
		recv: make(chan *fnpb.StateResponse, 10),
		send: make(chan *fnpb.StateRequest, 10),
	}
	sc := makeStateChannel(ctx, "state-port-1", fakeClient, func() {})
	mgr.ports["state-port-1"] = sc

	scoped := NewScopedStateManager(mgr, "inst-state-1")

	// OpenReader
	r, err := scoped.OpenReader(ctx, "state-port-1", &fnpb.StateKey{})
	if err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	_ = r

	// OpenWriter Append
	wAppend, err := scoped.OpenWriter(ctx, "state-port-1", &fnpb.StateKey{}, StateWriteAppend)
	if err != nil {
		t.Fatalf("OpenWriter Append failed: %v", err)
	}
	// OpenWriter Clear
	wClear, err := scoped.OpenWriter(ctx, "state-port-1", &fnpb.StateKey{}, StateWriteClear)
	if err != nil {
		t.Fatalf("OpenWriter Clear failed: %v", err)
	}

	// Test writing
	go func() {
		req := <-fakeClient.send
		fakeClient.recv <- &fnpb.StateResponse{Id: req.Id, Response: &fnpb.StateResponse_Append{Append: &fnpb.StateAppendResponse{}}}
		req2 := <-fakeClient.send
		fakeClient.recv <- &fnpb.StateResponse{Id: req2.Id, Response: &fnpb.StateResponse_Clear{Clear: &fnpb.StateClearResponse{}}}
	}()

	if _, err := wAppend.Write([]byte("append-val")); err != nil {
		t.Errorf("wAppend.Write error: %v", err)
	}
	if _, err := wClear.Write(nil); err != nil {
		t.Errorf("wClear.Write error: %v", err)
	}

	// Close scoped manager
	if err := scoped.Close(); err != nil {
		t.Errorf("scoped.Close failed: %v", err)
	}

	// Post-close calls should fail
	postCloseTests := []struct {
		name string
		op   func() error
	}{
		{
			name: "OpenReader_after_close",
			op: func() error {
				_, err := scoped.OpenReader(ctx, "state-port-1", &fnpb.StateKey{})
				return err
			},
		},
		{
			name: "OpenWriter_after_close",
			op: func() error {
				_, err := scoped.OpenWriter(ctx, "state-port-1", &fnpb.StateKey{}, StateWriteAppend)
				return err
			},
		},
	}

	for _, tc := range postCloseTests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.op(); err == nil {
				t.Errorf("%s succeeded, expected error after Close", tc.name)
			}
		})
	}
}

func TestStateChannelManager_CachingAndErrors(t *testing.T) {
	mgr := &StateChannelManager{
		ports: make(map[string]*StateChannel),
	}
	ch := &StateChannel{}
	mgr.ports["cached-url"] = ch

	tests := []struct {
		name    string
		url     string
		cancel  bool
		wantErr bool
		wantCh  *StateChannel
	}{
		{
			name:    "cached_channel",
			url:     "cached-url",
			cancel:  false,
			wantErr: false,
			wantCh:  ch,
		},
		{
			name:    "invalid_endpoint_error",
			url:     "invalid-endpoint:9999",
			cancel:  true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if tc.cancel {
				cancel()
			} else {
				defer cancel()
			}

			got, err := mgr.Open(ctx, tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("mgr.Open(%q) error = %v, wantErr = %v", tc.url, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.wantCh {
				t.Errorf("mgr.Open(%q) = %v, want %v", tc.url, got, tc.wantCh)
			}
		})
	}
}
