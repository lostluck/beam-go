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

package blobio

import (
	"bytes"
	"compress/gzip"
	"path/filepath"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	"lostluck.dev/beam-go"
)

func TestCompressionFromExt(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     compressionType
	}{
		{
			name:     "gzip_extension",
			filename: "file.gz",
			want:     compressionGzip,
		},
		{
			name:     "plain_text_extension",
			filename: "file.txt",
			want:     compressionUncompressed,
		},
		{
			name:     "tar_gz_extension",
			filename: "archive.tar.gz",
			want:     compressionGzip,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := compressionFromExt(tc.filename); got != tc.want {
				t.Errorf("compressionFromExt(%q) = %v, want %v", tc.filename, got, tc.want)
			}
		})
	}
}

func TestAllowEmptyMatch(t *testing.T) {
	tests := []struct {
		name      string
		pattern   string
		treatment emptyTreatment
		want      bool
	}{
		{
			name:      "allow_with_wildcard",
			pattern:   "*.txt",
			treatment: emptyAllow,
			want:      true,
		},
		{
			name:      "disallow_with_exact_filename",
			pattern:   "file.txt",
			treatment: emptyDisallow,
			want:      false,
		},
		{
			name:      "allow_if_wildcard_matching_wildcard",
			pattern:   "*.txt",
			treatment: emptyAllowIfWildcard,
			want:      true,
		},
		{
			name:      "allow_if_wildcard_matching_exact",
			pattern:   "file.txt",
			treatment: emptyAllowIfWildcard,
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := allowEmptyMatch(tc.pattern, tc.treatment); got != tc.want {
				t.Errorf("allowEmptyMatch(%q, %v) = %v, want %v", tc.pattern, tc.treatment, got, tc.want)
			}
		})
	}
}

func TestIsDirectory(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "forward_slash_trailing",
			path: "foo/bar/",
			want: true,
		},
		{
			name: "backslash_trailing",
			path: `foo/bar\`,
			want: true,
		},
		{
			name: "regular_file_path",
			path: "foo/bar/file.txt",
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDirectory(tc.path); got != tc.want {
				t.Errorf("isDirectory(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestGzipReader(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	bucketURL := "file://" + filepath.ToSlash(tmpDir)

	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = b.Close()
	}()

	// Write gzipped data
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte("hello gzipped world"))
	_ = zw.Close()

	if err := b.WriteAll(ctx, "test.gz", buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}

	rb := ReadableBlob{
		Metadata: BlobMetadata{
			Bucket:       bucketURL,
			Key:          "test.gz",
			Size:         int64(buf.Len()),
			LastModified: time.Now(),
		},
		Compression: compressionAuto,
	}

	content, err := rb.ReadString(ctx)
	if err != nil {
		t.Fatalf("ReadString error: %v", err)
	}
	if content != "hello gzipped world" {
		t.Errorf("got %q, want %q", content, "hello gzipped world")
	}

	// Test seek panics
	rc, err := rb.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = rc.Close()
	}()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected Seek to panic")
		}
	}()
	_, _ = rc.Seek(0, 0)
}

func TestReadableBlob_Uncompressed(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	bucketURL := "file://" + filepath.ToSlash(tmpDir)

	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = b.Close()
	}()

	if err := b.WriteAll(ctx, "data.txt", []byte("plain text content"), nil); err != nil {
		t.Fatal(err)
	}

	rb := ReadableBlob{
		Metadata: BlobMetadata{
			Bucket:       bucketURL,
			Key:          "data.txt",
			Size:         18,
			LastModified: time.Now(),
		},
		Compression: compressionUncompressed,
	}

	data, err := rb.Read(ctx)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if string(data) != "plain text content" {
		t.Errorf("got %q, want %q", string(data), "plain text content")
	}

	// Test Open with explicit compressionAuto on uncompressed
	rbAuto := rb
	rbAuto.Compression = compressionAuto
	strAuto, err := rbAuto.ReadString(ctx)
	if err != nil {
		t.Fatalf("ReadString error: %v", err)
	}
	if strAuto != "plain text content" {
		t.Errorf("got %q, want %q", strAuto, "plain text content")
	}

	// Test errors
	badBlob := ReadableBlob{
		Metadata: BlobMetadata{
			Bucket: "file://" + filepath.ToSlash(filepath.Join(tmpDir, "non-existent")),
			Key:    "no-file.txt",
		},
	}
	if _, err := badBlob.Open(ctx); err == nil {
		t.Errorf("expected error opening non-existent file")
	}
	if _, err := badBlob.Read(ctx); err == nil {
		t.Errorf("expected error reading non-existent file")
	}
	if _, err := badBlob.ReadString(ctx); err == nil {
		t.Errorf("expected error reading string from non-existent file")
	}
}

func TestNewDecompressionReader_Errors(t *testing.T) {
	_, err := newDecompressionReader(nil, compressionAuto)
	if err == nil {
		t.Errorf("expected error for compressionAuto in newDecompressionReader")
	}
}

type verifyBlobFn struct {
	Count beam.CounterInt64
}

func (fn *verifyBlobFn) ProcessBundle(dfc *beam.DFC[ReadableBlob]) error {
	ctx := dfc.Context()
	return dfc.Process(func(ec beam.ElmC, rb ReadableBlob) error {
		_, err := rb.ReadString(ctx)
		if err != nil {
			return err
		}
		fn.Count.Inc(dfc, 1)
		return nil
	})
}

func TestMatchAndReadPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	bucketURL := "file://" + filepath.ToSlash(tmpDir)

	ctx := t.Context()
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = b.Close()
	}()

	if err := b.WriteAll(ctx, "file1.txt", []byte("file 1 content"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteAll(ctx, "file2.txt", []byte("file 2 content"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteAll(ctx, "sub/file3.txt", []byte("file 3 content"), nil); err != nil {
		t.Fatal(err)
	}

	p, err := beam.LaunchAndWait(ctx, func(s *beam.Scope) error {
		// Test MatchFiles
		matches := MatchFiles(s, bucketURL, "*.txt", MatchEmptyAllow())
		// Test ReadMatches
		blobs := ReadMatches(s, matches, ReadAutoCompression(), ReadDirectorySkip())
		beam.ParDo(s, blobs, &verifyBlobFn{}, beam.Name("v1"))

		// Test MatchAll
		patterns := beam.Create(s, beam.Pair(bucketURL, "sub/*.txt"), beam.Pair(bucketURL, "missing/*.txt"))
		allMatches := MatchAll(s, patterns, MatchEmptyAllowIfWildcard())
		allBlobs := ReadMatches(s, allMatches, ReadUncompressed())
		beam.ParDo(s, allBlobs, &verifyBlobFn{}, beam.Name("v2"))
		return nil
	})
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if p.Counters["v1.Count"] != 2 {
		t.Errorf("got %v, want 2", p.Counters["v1.Count"])
	}
	if p.Counters["v2.Count"] != 1 {
		t.Errorf("got %v, want 1", p.Counters["v2.Count"])
	}
}

func TestReadOptionFns(t *testing.T) {
	tests := []struct {
		name         string
		opt          ReadOptionFn
		wantCompress compressionType
		wantDirTreat directoryTreatment
	}{
		{
			name:         "auto_compression",
			opt:          ReadAutoCompression(),
			wantCompress: compressionAuto,
		},
		{
			name:         "gzip_compression",
			opt:          ReadGzip(),
			wantCompress: compressionGzip,
		},
		{
			name:         "uncompressed",
			opt:          ReadUncompressed(),
			wantCompress: compressionUncompressed,
		},
		{
			name:         "directory_skip",
			opt:          ReadDirectorySkip(),
			wantDirTreat: directorySkip,
		},
		{
			name:         "directory_disallow",
			opt:          ReadDirectoryDisallow(),
			wantDirTreat: directoryDisallow,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ro := &readOption{}
			tc.opt(ro)
			if tc.wantCompress != 0 && ro.Compression != tc.wantCompress {
				t.Errorf("Compression = %v, want %v", ro.Compression, tc.wantCompress)
			}
			if tc.wantDirTreat != 0 && ro.DirectoryTreatment != tc.wantDirTreat {
				t.Errorf("DirectoryTreatment = %v, want %v", ro.DirectoryTreatment, tc.wantDirTreat)
			}
		})
	}
}

func TestMatchOptionFns(t *testing.T) {
	tests := []struct {
		name          string
		opt           MatchOptionFn
		wantTreatment emptyTreatment
	}{
		{
			name:          "empty_allow",
			opt:           MatchEmptyAllow(),
			wantTreatment: emptyAllow,
		},
		{
			name:          "empty_disallow",
			opt:           MatchEmptyDisallow(),
			wantTreatment: emptyDisallow,
		},
		{
			name:          "empty_allow_if_wildcard",
			opt:           MatchEmptyAllowIfWildcard(),
			wantTreatment: emptyAllowIfWildcard,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mo := &matchOption{}
			tc.opt(mo)
			if mo.EmptyTreatment != tc.wantTreatment {
				t.Errorf("EmptyTreatment = %v, want %v", mo.EmptyTreatment, tc.wantTreatment)
			}
		})
	}
}
