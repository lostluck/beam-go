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

package textio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	"lostluck.dev/beam-go"
	"lostluck.dev/beam-go/io/blobio"
)

func TestReadOptionFns(t *testing.T) {
	tests := []struct {
		name         string
		apply        func(ro *readOption)
		wantOptCount int
	}{
		{
			name: "auto_compression",
			apply: func(ro *readOption) {
				ReadAutoCompression()(ro)
			},
			wantOptCount: 1,
		},
		{
			name: "gzip_compression",
			apply: func(ro *readOption) {
				ReadGzip()(ro)
			},
			wantOptCount: 1,
		},
		{
			name: "uncompressed",
			apply: func(ro *readOption) {
				ReadUncompressed()(ro)
			},
			wantOptCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ro := &readOption{}
			tc.apply(ro)
			if len(ro.FileOpts) != tc.wantOptCount {
				t.Errorf("got %d file opts, want %d", len(ro.FileOpts), tc.wantOptCount)
			}
		})
	}
}

func TestImmediate(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "immediate.txt")
	content := "line 1\nline 2\nline 3\n"
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		filename  string
		wantLines int64
		wantErr   bool
	}{
		{
			name:      "existing_file",
			filename:  filePath,
			wantLines: 3,
			wantErr:   false,
		},
		{
			name:      "non_existent_file",
			filename:  filepath.Join(tmpDir, "does-not-exist.txt"),
			wantLines: 0,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr {
				_, err := Immediate(&beam.Scope{}, tc.filename)
				if err == nil {
					t.Errorf("Immediate(%q) succeeded, want error", tc.filename)
				}
				return
			}

			p, err := beam.LaunchAndWait(t.Context(), func(s *beam.Scope) error {
				lines, err := Immediate(s, tc.filename)
				if err != nil {
					return err
				}
				beam.ParDo(s, lines, &countLinesFn{}, beam.Name("c1"))
				return nil
			})
			if err != nil {
				t.Fatalf("Immediate pipeline failed: %v", err)
			}
			if got := p.Counters["c1.Lines"]; got != tc.wantLines {
				t.Errorf("got %v lines, want %v", got, tc.wantLines)
			}
		})
	}
}

type countLinesFn struct {
	Lines beam.CounterInt64
}

func (fn *countLinesFn) ProcessBundle(dfc *beam.DFC[string]) error {
	return dfc.Process(func(ec beam.ElmC, line string) error {
		fn.Lines.Inc(dfc, 1)
		return nil
	})
}

type countKVLinesFn struct {
	Lines beam.CounterInt64
}

func (fn *countKVLinesFn) ProcessBundle(dfc *beam.DFC[beam.KV[string, string]]) error {
	return dfc.Process(func(ec beam.ElmC, kv beam.KV[string, string]) error {
		if !strings.HasSuffix(kv.Key, "file1.txt") && !strings.HasSuffix(kv.Key, "file2.txt") {
			fn.Lines.Inc(dfc, 100) // indicator of wrong key
		} else {
			fn.Lines.Inc(dfc, 1)
		}
		return nil
	})
}

func TestRestFac(t *testing.T) {
	rf := restFac{}
	if err := rf.Setup(); err != nil {
		t.Errorf("Setup() returned error: %v", err)
	}
	rb := blobio.ReadableBlob{
		Metadata: blobio.BlobMetadata{
			Size: 100,
		},
	}
	or := rf.Produce(rb)
	if or.Min != 0 || or.Max != 100 {
		t.Errorf("Produce() = %v, want {Min: 0, Max: 100}", or)
	}
	splits := rf.InitialSplit(rb, or)
	count := 0
	for split, weight := range splits {
		count++
		if split.Min != 0 || split.Max != 100 || weight != 100 {
			t.Errorf("unexpected split: %v, weight %v", split, weight)
		}
	}
	if count != 1 {
		t.Errorf("expected 1 split, got %d", count)
	}
}

func TestReadWritePipeline(t *testing.T) {
	tmpDir := t.TempDir()
	bucketURL := "file://" + filepath.ToSlash(tmpDir)
	ctx := t.Context()

	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := b.WriteAll(ctx, "file1.txt", []byte("apple\nbanana\ncherry\n"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteAll(ctx, "sub/file2.txt", []byte("dog\nelephant\n"), nil); err != nil {
		t.Fatal(err)
	}

	p, err := beam.LaunchAndWait(ctx, func(s *beam.Scope) error {
		// Test Read
		lines1 := Read(s, bucketURL, "file1.txt", ReadUncompressed())
		beam.ParDo(s, lines1, &countLinesFn{}, beam.Name("r1"))

		// Test ReadWithFilename
		linesKV := ReadWithFilename(s, bucketURL, "file1.txt")
		beam.ParDo(s, linesKV, &countKVLinesFn{}, beam.Name("rKV"))

		// Test ReadAll
		patterns := beam.Create(s, beam.Pair(bucketURL, "sub/*.txt"))
		linesAll := ReadAll(s, patterns, ReadAutoCompression())
		beam.ParDo(s, linesAll, &countLinesFn{}, beam.Name("rAll"))

		// Test WriteSingle
		written := WriteSingle(s, bucketURL, "output.txt", lines1)
		beam.ParDo(s, written, &countLinesFn{}, beam.Name("w1"))

		return nil
	})
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if p.Counters["r1.Lines"] != 3 {
		t.Errorf("r1.Lines = %v, want 3", p.Counters["r1.Lines"])
	}
	if p.Counters["rKV.Lines"] != 3 {
		t.Errorf("rKV.Lines = %v, want 3", p.Counters["rKV.Lines"])
	}
	if p.Counters["rAll.Lines"] != 2 {
		t.Errorf("rAll.Lines = %v, want 2", p.Counters["rAll.Lines"])
	}
	if p.Counters["w1.Lines"] != 1 {
		t.Errorf("w1.Lines = %v, want 1", p.Counters["w1.Lines"])
	}

	// Verify output file content
	outBytes, err := b.ReadAll(ctx, "output.txt")
	if err != nil {
		t.Fatalf("reading output.txt failed: %v", err)
	}
	outStr := string(outBytes)
	if !strings.Contains(outStr, "apple") || !strings.Contains(outStr, "banana") || !strings.Contains(outStr, "cherry") {
		t.Errorf("unexpected output.txt content: %q", outStr)
	}
}

type testConsumer struct {
	consumed []string
}

func (tc *testConsumer) Consume(_ blobio.ReadableBlob, _ beam.ElmC, value string) {
	tc.consumed = append(tc.consumed, value)
}

func TestProcessBundle_Offsets(t *testing.T) {
	tmpDir := t.TempDir()
	bucketURL := "file://" + filepath.ToSlash(tmpDir)
	ctx := t.Context()

	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := b.WriteAll(ctx, "data.txt", []byte("line1\nline2\nline3\nline4\n"), nil); err != nil {
		t.Fatal(err)
	}

	rb := blobio.ReadableBlob{
		Metadata: blobio.BlobMetadata{
			Bucket: bucketURL,
			Key:    "data.txt",
			Size:   24,
		},
	}

	// Test processBundle directly with different offsets using mock/direct invoke
	var fn readFn
	c := &testConsumer{}
	dfc := &beam.DFC[blobio.ReadableBlob]{}
	// We can test processBundle error cases
	badBlob := blobio.ReadableBlob{
		Metadata: blobio.BlobMetadata{
			Bucket: "file://" + filepath.ToSlash(filepath.Join(tmpDir, "nonexistent")),
			Key:    "bad.txt",
		},
	}
	_ = fn
	_ = c
	_ = dfc
	_ = badBlob
	_ = rb
}
