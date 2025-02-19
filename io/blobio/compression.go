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
	"compress/gzip"
	"io"
	"log/slog"
)

// gzipReader is a wrapper around a gzip.Reader that also closes the underlying io.ReadCloser.
type gzipReader struct {
	rc io.ReadCloser
	zr *gzip.Reader
}

// newGzipReader creates a new gzipReader from an io.ReadCloser.
func newGzipReader(rc io.ReadSeekCloser) (*gzipReader, error) {
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return nil, err
	}
	return &gzipReader{rc: rc, zr: zr}, nil
}

// Seek reads from the gzip reader.
func (r *gzipReader) Seek(offset int64, whence int) (int64, error) {
	panic("not implemented") // TODO: Implement
}

// Read reads from the gzip reader.
func (r *gzipReader) Read(p []byte) (int, error) {
	return r.zr.Read(p)
}

// Close closes the gzip reader and the underlying io.ReadCloser.
func (r *gzipReader) Close() (err error) {
	defer func() {
		rcErr := r.rc.Close()
		if err != nil {
			if rcErr != nil {
				slog.Error("error closing reader", "error", rcErr)
			}
			return
		}
		err = rcErr
	}()

	return r.zr.Close()
}
