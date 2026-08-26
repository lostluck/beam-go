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

package prism

import (
	"archive/zip"
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestConstructDownloadPath(t *testing.T) {
	tests := []struct {
		name           string
		release        string
		tag            string
		wantContains   string
		wantNotContain string
	}{
		{
			name:         "standard_release",
			release:      "v2.69.0",
			tag:          "v2.69.0",
			wantContains: "v2.69.0",
		},
		{
			name:           "rc_candidate_stripped",
			release:        "v2.69.0",
			tag:            "v2.69.0-RC1",
			wantContains:   "v2.69.0",
			wantNotContain: "-RC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := constructDownloadPath(tc.release, tc.tag)
			if !strings.Contains(path, tc.wantContains) {
				t.Errorf("constructDownloadPath(%q, %q) = %q, want containing %q", tc.release, tc.tag, path, tc.wantContains)
			}
			if tc.wantNotContain != "" && strings.Contains(path, tc.wantNotContain) {
				t.Errorf("constructDownloadPath(%q, %q) = %q, want not containing %q", tc.release, tc.tag, path, tc.wantNotContain)
			}
		})
	}
}

func TestPickPort(t *testing.T) {
	portStr := pickPort()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("pickPort() = %q, not an int: %v", portStr, err)
	}
	if port <= 0 || port > 65535 {
		t.Errorf("port %d out of valid range", port)
	}
}

func TestDownloadToCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("downloaded content"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		url         string
		destPath    string
		wantErr     bool
		wantContent string
	}{
		{
			name:        "successful_download",
			url:         ts.URL + "/ok",
			destPath:    filepath.Join(tmpDir, "ok.zip"),
			wantErr:     false,
			wantContent: "downloaded content",
		},
		{
			name:     "http_404_not_found",
			url:      ts.URL + "/bad",
			destPath: filepath.Join(tmpDir, "err.zip"),
			wantErr:  true,
		},
		{
			name:     "invalid_dest_path",
			url:      ts.URL + "/ok",
			destPath: filepath.Join(tmpDir, "nonexistent-dir", "sub", "bad.zip"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := downloadToCache(tc.url, tc.destPath)
			if (err != nil) != tc.wantErr {
				t.Fatalf("downloadToCache(%q, %q) error = %v, wantErr = %v", tc.url, tc.destPath, err, tc.wantErr)
			}
			if !tc.wantErr && tc.wantContent != "" {
				data, err := os.ReadFile(tc.destPath)
				if err != nil || string(data) != tc.wantContent {
					t.Errorf("file content mismatch: got %q, want %q", string(data), tc.wantContent)
				}
			}
		})
	}
}

func TestUnzipCachedFile(t *testing.T) {
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "archive.zip")

	// Create a valid zip
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	fw, err := zw.Create("inner_binary")
	if err != nil {
		t.Fatal(err)
	}
	fw.Write([]byte("#!/bin/sh\nexit 0\n"))
	zw.Close()

	if err := os.WriteFile(zipPath, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}

	nonZip := filepath.Join(tmpDir, "notazip.txt")
	os.WriteFile(nonZip, []byte("raw binary content"), 0755)

	tests := []struct {
		name     string
		srcZip   string
		destDir  string
		wantErr  bool
		wantBase string
		errIs    error
	}{
		{
			name:     "valid_zip",
			srcZip:   zipPath,
			destDir:  outDir,
			wantErr:  false,
			wantBase: "inner_binary",
		},
		{
			name:     "already_extracted",
			srcZip:   zipPath,
			destDir:  outDir,
			wantErr:  false,
			wantBase: "inner_binary",
		},
		{
			name:    "invalid_zip_format",
			srcZip:  nonZip,
			destDir: outDir,
			wantErr: true,
			errIs:   zip.ErrFormat,
		},
		{
			name:    "non_existent_zip",
			srcZip:  filepath.Join(tmpDir, "does-not-exist.zip"),
			destDir: outDir,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			extracted, err := unzipCachedFile(tc.srcZip, tc.destDir)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unzipCachedFile error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.errIs != nil && !errors.Is(err, tc.errIs) {
				t.Errorf("error = %v, want error %v", err, tc.errIs)
			}
			if !tc.wantErr && tc.wantBase != "" && filepath.Base(extracted) != tc.wantBase {
				t.Errorf("extracted binary = %s, want base %s", extracted, tc.wantBase)
			}
		})
	}
}

func TestHandle(t *testing.T) {
	called := false
	h := &Handle{
		addr: "localhost:1234",
		cancelFn: func() {
			called = true
		},
	}
	if h.Addr() != "localhost:1234" {
		t.Errorf("Addr() = %v, want localhost:1234", h.Addr())
	}
	h.Terminate()
	if !called {
		t.Errorf("Terminate() did not invoke cancelFn")
	}
}

func TestStart_CachedAndRunning(t *testing.T) {
	ctx := t.Context()
	// Start with default Options
	h1, err := Start(ctx, Options{})
	if err != nil {
		t.Fatalf("Start(Options{}) failed: %v", err)
	}
	if h1.Addr() == "" {
		t.Errorf("expected non-empty addr")
	}

	// Test cached handle retrieval
	h2, err := Start(ctx, Options{})
	if err != nil {
		t.Fatalf("Start(Options{}) second call failed: %v", err)
	}
	if h1.Addr() != h2.Addr() {
		t.Errorf("expected same cached handle, got %v and %v", h1.Addr(), h2.Addr())
	}

	// Test Start with custom invalid location
	_, err = Start(ctx, Options{
		Location: "/path/to/nonexistent/prism_bin",
	})
	if err == nil {
		t.Errorf("expected error starting non-existent prism binary")
	}
}

func TestStart_SpecificPort(t *testing.T) {
	ctx := t.Context()
	port := pickPort()
	h, err := Start(ctx, Options{Port: port})
	if err != nil {
		t.Fatalf("Start with port %v failed: %v", port, err)
	}
	if !strings.HasSuffix(h.Addr(), ":"+port) {
		t.Errorf("expected addr to end with :%s, got %s", port, h.Addr())
	}
	defer h.Terminate()
	time.Sleep(100 * time.Millisecond)
}
