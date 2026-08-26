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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	fnpb "lostluck.dev/beam-go/internal/model/fnexecution_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

func TestHandleInstruction_Units(t *testing.T) {
	ctx := t.Context()
	logChan := make(chan *fnpb.LogEntry, 10)

	t.Run("ProcessBundle_Success", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}
		ctrl.exec = func(ctx context.Context, c *Control, dc DataContext) (*fnpb.ProcessBundleResponse, error) {
			return &fnpb.ProcessBundleResponse{}, nil
		}
		ctrl.RegisterMonitor(DataContext{instID: "inst-1"}, func(logger *slog.Logger) (map[string]*pipepb.MonitoringInfo, map[string][]byte) {
			return map[string]*pipepb.MonitoringInfo{"m1": {Urn: "urn:test"}}, map[string][]byte{"m1": []byte("val")}
		})
		pbReq := &fnpb.InstructionRequest{
			InstructionId: "inst-1",
			Request: &fnpb.InstructionRequest_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleRequest{
					ProcessBundleDescriptorId: "desc-1",
				},
			},
		}
		resp := handleInstruction(ctx, pbReq, ctrl, logChan)
		if resp.GetError() != "" || resp.GetProcessBundle() == nil {
			t.Errorf("ProcessBundle failed: %v", resp)
		}
	})

	t.Run("ProcessBundle_Error", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}
		ctrl.exec = func(ctx context.Context, c *Control, dc DataContext) (*fnpb.ProcessBundleResponse, error) {
			return nil, fmt.Errorf("exec error")
		}
		pbReq := &fnpb.InstructionRequest{
			InstructionId: "inst-1",
			Request: &fnpb.InstructionRequest_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleRequest{
					ProcessBundleDescriptorId: "desc-1",
				},
			},
		}
		respErr := handleInstruction(ctx, pbReq, ctrl, logChan)
		if respErr.GetError() == "" {
			t.Errorf("expected error for failed exec")
		}
	})

	t.Run("FinalizeBundle", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}
		finReq := &fnpb.InstructionRequest{
			InstructionId: "inst-2",
			Request: &fnpb.InstructionRequest_FinalizeBundle{
				FinalizeBundle: &fnpb.FinalizeBundleRequest{
					InstructionId: "inst-1",
				},
			},
		}
		finResp := handleInstruction(ctx, finReq, ctrl, logChan)
		if finResp.GetFinalizeBundle() == nil {
			t.Errorf("FinalizeBundle failed: %v", finResp)
		}
	})

	t.Run("ProcessBundleProgress", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}
		ctrl.RegisterMonitor(DataContext{instID: "inst-1"}, func(logger *slog.Logger) (map[string]*pipepb.MonitoringInfo, map[string][]byte) {
			return map[string]*pipepb.MonitoringInfo{"m1": {Urn: "urn:test"}}, map[string][]byte{"m1": []byte("val")}
		})

		tests := []struct {
			name        string
			instID      string
			wantDataLen int
		}{
			{name: "with_monitor", instID: "inst-1", wantDataLen: 1},
			{name: "missing_monitor", instID: "inst-missing", wantDataLen: 0},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := &fnpb.InstructionRequest{
					InstructionId: "prog-req",
					Request: &fnpb.InstructionRequest_ProcessBundleProgress{
						ProcessBundleProgress: &fnpb.ProcessBundleProgressRequest{
							InstructionId: tc.instID,
						},
					},
				}
				resp := handleInstruction(ctx, req, ctrl, logChan)
				if resp.GetProcessBundleProgress() == nil {
					t.Fatalf("ProcessBundleProgress returned nil")
				}
				if len(resp.GetProcessBundleProgress().GetMonitoringData()) != tc.wantDataLen {
					t.Errorf("monitoring data len = %d, want %d", len(resp.GetProcessBundleProgress().GetMonitoringData()), tc.wantDataLen)
				}
			})
		}
	})

	t.Run("ProcessBundleSplit", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}

		tests := []struct {
			name   string
			setup  func()
			instID string
		}{
			{
				name:   "missing_splitter",
				setup:  func() {},
				instID: "inst-missing",
			},
			{
				name: "valid_splitter",
				setup: func() {
					ctrl.RegisterSplitter(DataContext{instID: "inst-1"}, func(m map[string]*fnpb.ProcessBundleSplitRequest_DesiredSplit) (*fnpb.ProcessBundleSplitResponse, error) {
						return &fnpb.ProcessBundleSplitResponse{}, nil
					})
				},
				instID: "inst-1",
			},
			{
				name: "splitter_error_fallback",
				setup: func() {
					ctrl.RegisterSplitter(DataContext{instID: "inst-1"}, func(m map[string]*fnpb.ProcessBundleSplitRequest_DesiredSplit) (*fnpb.ProcessBundleSplitResponse, error) {
						return nil, fmt.Errorf("split err")
					})
				},
				instID: "inst-1",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tc.setup()
				req := &fnpb.InstructionRequest{
					InstructionId: "split-req",
					Request: &fnpb.InstructionRequest_ProcessBundleSplit{
						ProcessBundleSplit: &fnpb.ProcessBundleSplitRequest{
							InstructionId: tc.instID,
						},
					},
				}
				resp := handleInstruction(ctx, req, ctrl, logChan)
				if resp.GetProcessBundleSplit() == nil {
					t.Errorf("ProcessBundleSplit returned nil")
				}
			})
		}
	})

	t.Run("MonitoringInfos_And_Unexpected", func(t *testing.T) {
		ctrl := &Control{
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
			plans:       map[bundleDescriptorID][]any{},
			monitors:    map[instructionID]Monitor{},
			active:      map[instructionID]Splitter{},
		}
		ctrl.RegisterMonitor(DataContext{instID: "inst-1"}, func(logger *slog.Logger) (map[string]*pipepb.MonitoringInfo, map[string][]byte) {
			return map[string]*pipepb.MonitoringInfo{"m1": {Urn: "urn:test"}}, map[string][]byte{"m1": []byte("val")}
		})

		// MonitoringInfos
		monReq := &fnpb.InstructionRequest{
			InstructionId: "inst-7",
			Request: &fnpb.InstructionRequest_MonitoringInfos{
				MonitoringInfos: &fnpb.MonitoringInfosMetadataRequest{
					MonitoringInfoId: []string{"m1"},
				},
			},
		}
		monResp := handleInstruction(ctx, monReq, ctrl, logChan)
		if monResp.GetMonitoringInfos() == nil || monResp.GetMonitoringInfos().GetMonitoringInfo()["m1"] == nil {
			t.Errorf("MonitoringInfos failed: %v", monResp)
		}

		// HarnessMonitoringInfos
		hmonReq := &fnpb.InstructionRequest{
			InstructionId: "inst-8",
			Request: &fnpb.InstructionRequest_HarnessMonitoringInfos{
				HarnessMonitoringInfos: &fnpb.HarnessMonitoringInfosRequest{},
			},
		}
		hmonResp := handleInstruction(ctx, hmonReq, ctrl, logChan)
		if hmonResp.GetHarnessMonitoringInfos() == nil {
			t.Errorf("HarnessMonitoringInfos failed: %v", hmonResp)
		}

		// Unexpected
		unexpReq := &fnpb.InstructionRequest{InstructionId: "inst-9"}
		unexpResp := handleInstruction(ctx, unexpReq, ctrl, logChan)
		if unexpResp.GetError() == "" {
			t.Errorf("expected error for unexpected request")
		}
	})
}

func TestControl_GetOrLookupPlan(t *testing.T) {
	ctrl := &Control{
		descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{
			"cached-desc": {Id: "cached-desc"},
		},
		plans: map[bundleDescriptorID][]any{
			"cached-plan": {"plan1", "plan2"},
		},
		fetchBD: func(id bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error) {
			if id == "err-desc" {
				return nil, errors.New("not found")
			}
			return &fnpb.ProcessBundleDescriptor{Id: string(id)}, nil
		},
	}

	tests := []struct {
		name    string
		bdID    bundleDescriptorID
		want    any
		wantErr bool
	}{
		{
			name:    "cached_plan",
			bdID:    "cached-plan",
			want:    "plan1",
			wantErr: false,
		},
		{
			name:    "cached_descriptor",
			bdID:    "cached-desc",
			want:    "built:cached-desc",
			wantErr: false,
		},
		{
			name:    "fetch_descriptor_success",
			bdID:    "fetch-desc",
			want:    "built:fetch-desc",
			wantErr: false,
		},
		{
			name:    "fetch_descriptor_failure",
			bdID:    "err-desc",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ctrl.GetOrLookupPlan(DataContext{bdID: tc.bdID}, func(pbd *fnpb.ProcessBundleDescriptor) any {
				return "built:" + pbd.Id
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetOrLookupPlan(%v) error = %v, wantErr = %v", tc.bdID, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("GetOrLookupPlan(%v) = %v, want %v", tc.bdID, got, tc.want)
			}
		})
	}
}

func TestDefaultDial_Error(t *testing.T) {
	ctx := t.Context()
	_, err := DefaultDial(ctx, "invalid-endpoint-nonexistent:9999", 50*time.Millisecond)
	if err == nil {
		t.Errorf("expected error dialing invalid endpoint")
	}
}

type mockBeamFnServer struct {
	fnpb.UnimplementedBeamFnControlServer
	fnpb.UnimplementedBeamFnLoggingServer

	reqs []*fnpb.InstructionRequest
}

func (s *mockBeamFnServer) Control(stream fnpb.BeamFnControl_ControlServer) error {
	for _, req := range s.reqs {
		if err := stream.Send(req); err != nil {
			return err
		}
		// Receive response
		_, err := stream.Recv()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *mockBeamFnServer) Logging(stream fnpb.BeamFnLogging_LoggingServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			return nil
		}
	}
}

func TestMain_HarnessIntegration(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lis.Close()
	}()

	mockSrv := &mockBeamFnServer{
		reqs: []*fnpb.InstructionRequest{
			{
				InstructionId: "inst-pb",
				Request: &fnpb.InstructionRequest_ProcessBundle{
					ProcessBundle: &fnpb.ProcessBundleRequest{
						ProcessBundleDescriptorId: "desc-1",
					},
				},
			},
			{
				InstructionId: "inst-fin",
				Request: &fnpb.InstructionRequest_FinalizeBundle{
					FinalizeBundle: &fnpb.FinalizeBundleRequest{
						InstructionId: "inst-pb",
					},
				},
			},
		},
	}

	grpcServer := grpc.NewServer()
	fnpb.RegisterBeamFnControlServer(grpcServer, mockSrv)
	fnpb.RegisterBeamFnLoggingServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	endpoint := lis.Addr().String()

	exec := func(ctx context.Context, ctrl *Control, dc DataContext) (*fnpb.ProcessBundleResponse, error) {
		dc.logger.Info("executing bundle", "instID", dc.instID)
		return &fnpb.ProcessBundleResponse{}, nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = Main(ctx, endpoint, Options{
		LoggingEndpoint: endpoint,
	}, exec)
	if err != nil {
		t.Fatalf("Main returned error: %v", err)
	}
}
