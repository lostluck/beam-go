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

package universal

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/beamopts"
	jobpb "lostluck.dev/beam-go/internal/model/jobmanagement_v1"
	pipepb "lostluck.dev/beam-go/internal/model/pipeline_v1"
)

func TestGetJobName(t *testing.T) {
	tests := []struct {
		name     string
		opts     beamopts.Struct
		wantName string
		prefix   string
	}{
		{
			name:     "custom_name",
			opts:     beamopts.Struct{Name: "custom-job"},
			wantName: "custom-job",
		},
		{
			name:   "auto_generated_name",
			opts:   beamopts.Struct{},
			prefix: "go-job-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getJobName(tc.opts)
			if tc.wantName != "" && got != tc.wantName {
				t.Errorf("getJobName(%+v) = %q, want %q", tc.opts, got, tc.wantName)
			}
			if tc.prefix != "" && !strings.HasPrefix(got, tc.prefix) {
				t.Errorf("getJobName(%+v) = %q, want prefix %q", tc.opts, got, tc.prefix)
			}
		})
	}
}

func TestWriteWorkerID(t *testing.T) {
	ctx := t.Context()
	ctx1 := writeWorkerID(ctx, "worker-1")
	md, ok := metadata.FromOutgoingContext(ctx1)
	if !ok || len(md[idKey]) == 0 || md[idKey][0] != "worker-1" {
		t.Errorf("failed to write worker ID: %v", md)
	}

	// Merge with existing metadata
	ctx2 := writeWorkerID(ctx1, "worker-2")
	md2, _ := metadata.FromOutgoingContext(ctx2)
	if len(md2[idKey]) < 1 {
		t.Errorf("failed to merge worker ID metadata")
	}
}

func TestDisabledHandler(t *testing.T) {
	tests := []struct {
		name  string
		level slog.Level
	}{
		{name: "debug", level: slog.LevelDebug},
		{name: "info", level: slog.LevelInfo},
		{name: "warn", level: slog.LevelWarn},
		{name: "error", level: slog.LevelError},
	}

	h := disabledHandler{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if h.Enabled(t.Context(), tc.level) {
				t.Errorf("disabledHandler.Enabled(..., %v) = true, want false", tc.level)
			}
		})
	}
}

func TestMessageSeverity(t *testing.T) {
	tests := []struct {
		name string
		imp  jobpb.JobMessage_MessageImportance
		want slog.Level
	}{
		{"error", jobpb.JobMessage_JOB_MESSAGE_ERROR, slog.LevelError},
		{"warning", jobpb.JobMessage_JOB_MESSAGE_WARNING, slog.LevelWarn},
		{"basic", jobpb.JobMessage_JOB_MESSAGE_BASIC, slog.LevelInfo},
		{"debug", jobpb.JobMessage_JOB_MESSAGE_DEBUG, slog.LevelDebug},
		{"detailed", jobpb.JobMessage_JOB_MESSAGE_DETAILED, slog.LevelDebug},
		{"unknown_defaults_to_info", jobpb.JobMessage_MessageImportance(99), slog.LevelInfo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := messageSeverity(tc.imp); got != tc.want {
				t.Errorf("messageSeverity(%v) = %v, want %v", tc.imp, got, tc.want)
			}
		})
	}
}

func TestResults(t *testing.T) {
	pipe := &pipepb.Pipeline{
		Components: &pipepb.Components{
			Transforms: map[string]*pipepb.PTransform{
				"t1": {UniqueName: "MyTransform"},
			},
		},
	}

	// Counter payload
	counterPayload := protowire.AppendVarint(nil, 42)

	// Distribution payload: count, sum, min, max
	enc := coders.NewEncoder()
	enc.Varint(5)
	enc.Varint(100)
	enc.Varint(10)
	enc.Varint(30)
	distBuf := enc.Data()

	committed := []*pipepb.MonitoringInfo{
		{
			Urn:     "beam:metric:user:sum_int64:v1",
			Type:    "beam:metrics:sum_int64:v1",
			Payload: counterPayload,
			Labels: map[string]string{
				"PTRANSFORM": "t1",
				"NAME":       "my_counter",
			},
		},
		{
			Urn:     "beam:metric:user:distribution_int64:v1",
			Type:    "beam:metrics:distribution_int64:v1",
			Payload: distBuf,
			Labels: map[string]string{
				"PTRANSFORM": "t1",
				"NAME":       "my_dist",
			},
		},
		{
			Urn:     "other:metric",
			Type:    "other",
			Payload: counterPayload,
		},
	}

	res := &Results{
		pipe: pipe,
		res: &jobpb.MetricResults{
			Committed: committed,
		},
	}

	counters := res.UserCounters()
	if counters["MyTransform.my_counter"] != 42 {
		t.Errorf("got counter %v, want 42", counters["MyTransform.my_counter"])
	}

	dists := res.UserDistributions()
	dist, ok := dists["MyTransform.my_dist"]
	if !ok {
		t.Fatalf("my_dist not found in UserDistributions")
	}
	if dist.Count != 5 || dist.Sum != 100 || dist.Min != 10 || dist.Max != 30 {
		t.Errorf("unexpected distribution: %+v", dist)
	}

	if committedVal := res.Committed("t1.my_counter"); committedVal != 42 {
		t.Errorf("Committed(t1.my_counter) = %v, want 42", committedVal)
	}
	if committedVal := res.Committed("nonexistent"); committedVal != 0 {
		t.Errorf("Committed(nonexistent) = %v, want 0", committedVal)
	}
}

func TestStageFile_Errors(t *testing.T) {
	err := stageFile("/path/to/nonexistent/file.bin", nil)
	if err == nil {
		t.Errorf("expected error staging non-existent file")
	}
}

// Mock JobService and ArtifactStagingService for testing Execute, Wait, Cancel, Metrics, stageViaPortableAPI
type mockJobServer struct {
	jobpb.UnimplementedJobServiceServer
	jobpb.UnimplementedArtifactStagingServiceServer

	addr string
	msgs []*jobpb.JobMessagesResponse
}

func (s *mockJobServer) Prepare(ctx context.Context, req *jobpb.PrepareJobRequest) (*jobpb.PrepareJobResponse, error) {
	return &jobpb.PrepareJobResponse{
		PreparationId:           "prep-1",
		StagingSessionToken:     "token-1",
		ArtifactStagingEndpoint: &pipepb.ApiServiceDescriptor{Url: s.addr},
	}, nil
}

func (s *mockJobServer) Run(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.RunJobResponse, error) {
	return &jobpb.RunJobResponse{
		JobId: "job-123",
	}, nil
}

func (s *mockJobServer) Cancel(ctx context.Context, req *jobpb.CancelJobRequest) (*jobpb.CancelJobResponse, error) {
	return &jobpb.CancelJobResponse{
		State: jobpb.JobState_CANCELLED,
	}, nil
}

func (s *mockJobServer) GetJobMetrics(ctx context.Context, req *jobpb.GetJobMetricsRequest) (*jobpb.GetJobMetricsResponse, error) {
	return &jobpb.GetJobMetricsResponse{
		Metrics: &jobpb.MetricResults{
			Committed: []*pipepb.MonitoringInfo{
				{
					Urn:     "beam:metric:user:sum_int64:v1",
					Type:    "beam:metrics:sum_int64:v1",
					Payload: protowire.AppendVarint(nil, 100),
					Labels: map[string]string{
						"PTRANSFORM": "t1",
						"NAME":       "count",
					},
				},
			},
		},
	}, nil
}

func (s *mockJobServer) GetMessageStream(req *jobpb.JobMessagesRequest, stream jobpb.JobService_GetMessageStreamServer) error {
	for _, msg := range s.msgs {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockJobServer) ReverseArtifactRetrievalService(stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	// First recv token
	wrapper, err := stream.Recv()
	if err != nil {
		return err
	}
	_ = wrapper

	// Request artifact resolution
	err = stream.Send(&jobpb.ArtifactRequestWrapper{
		Request: &jobpb.ArtifactRequestWrapper_ResolveArtifact{
			ResolveArtifact: &jobpb.ResolveArtifactsRequest{
				Artifacts: []*pipepb.ArtifactInformation{},
			},
		},
	})
	if err != nil {
		return err
	}

	// Recv resolution response
	_, err = stream.Recv()
	if err != nil {
		return err
	}

	return nil
}

func TestPipeline_UniversalRunner_Mock(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	mockSrv := &mockJobServer{
		addr: lis.Addr().String(),
		msgs: []*jobpb.JobMessagesResponse{
			{
				Response: &jobpb.JobMessagesResponse_MessageResponse{
					MessageResponse: &jobpb.JobMessage{
						Importance:  jobpb.JobMessage_JOB_MESSAGE_BASIC,
						MessageText: "Job started",
					},
				},
			},
			{
				Response: &jobpb.JobMessagesResponse_StateResponse{
					StateResponse: &jobpb.JobStateEvent{
						State: jobpb.JobState_DONE,
					},
				},
			},
		},
	}

	grpcServer := grpc.NewServer()
	jobpb.RegisterJobServiceServer(grpcServer, mockSrv)
	jobpb.RegisterArtifactStagingServiceServer(grpcServer, mockSrv)

	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	ctx := t.Context()
	pipe := &pipepb.Pipeline{
		Components: &pipepb.Components{
			Transforms: map[string]*pipepb.PTransform{
				"t1": {UniqueName: "T1"},
			},
		},
	}

	p, err := Execute(ctx, pipe, beamopts.Struct{Endpoint: lis.Addr().String()})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer p.close()

	// Test Wait
	if err := p.Wait(ctx); err != nil {
		t.Errorf("Wait failed: %v", err)
	}

	// Test Cancel
	state, err := p.Cancel(ctx)
	if err != nil || state != jobpb.JobState_CANCELLED {
		t.Errorf("Cancel failed: state %v, err %v", state, err)
	}

	// Test Metrics
	metrics, err := p.Metrics(ctx)
	if err != nil {
		t.Fatalf("Metrics failed: %v", err)
	}
	if metrics.UserCounters()["T1.count"] != 100 {
		t.Errorf("got %v, want 100", metrics.UserCounters()["T1.count"])
	}
}

func TestWaitForCompletion_Failed(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	mockSrv := &mockJobServer{
		msgs: []*jobpb.JobMessagesResponse{
			{
				Response: &jobpb.JobMessagesResponse_MessageResponse{
					MessageResponse: &jobpb.JobMessage{
						Importance:  jobpb.JobMessage_JOB_MESSAGE_ERROR,
						MessageText: "Fatal job error occurred",
					},
				},
			},
			{
				Response: &jobpb.JobMessagesResponse_StateResponse{
					StateResponse: &jobpb.JobStateEvent{
						State: jobpb.JobState_FAILED,
					},
				},
			},
		},
	}

	grpcServer := grpc.NewServer()
	jobpb.RegisterJobServiceServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := jobpb.NewJobServiceClient(conn)
	logger := slog.Default()

	err = waitForCompletion(t.Context(), logger, client, "job-fail")
	if err == nil || !strings.Contains(err.Error(), "Fatal job error occurred") {
		t.Errorf("expected job failure error, got: %v", err)
	}
}

func TestStageViaPortableAPI_WithFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "artifact.bin")
	if err := os.WriteFile(testFile, []byte("artifact binary content"), 0644); err != nil {
		t.Fatal(err)
	}

	payload, _ := proto.Marshal(&pipepb.ArtifactFilePayload{Path: testFile})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	srv := &fileArtifactServer{
		typeUrn:     "beam:artifact:type:file:v1",
		typePayload: payload,
	}
	grpcServer := grpc.NewServer()
	jobpb.RegisterArtifactStagingServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = stageViaPortableAPI(t.Context(), conn, "test-token")
	if err != nil {
		t.Fatalf("stageViaPortableAPI failed: %v", err)
	}
}

type fileArtifactServer struct {
	jobpb.UnimplementedArtifactStagingServiceServer
	typeUrn     string
	typePayload []byte
}

func (s *fileArtifactServer) ReverseArtifactRetrievalService(stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	// Receive token
	_, err := stream.Recv()
	if err != nil {
		return err
	}

	// Send GetArtifact request
	err = stream.Send(&jobpb.ArtifactRequestWrapper{
		Request: &jobpb.ArtifactRequestWrapper_GetArtifact{
			GetArtifact: &jobpb.GetArtifactRequest{
				Artifact: &pipepb.ArtifactInformation{
					TypeUrn:     s.typeUrn,
					TypePayload: s.typePayload,
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Read chunks until last
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.IsLast {
			return nil
		}
	}
}
