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

// Package prism downloads, unzips, boots up a prism binary to run a pipeline against.
package prism

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// TODO: Allow configuration of port/ return of auto selected ports.
// TODO: Allow multiple jobs to hit the same process, but clean up when
// they are all done (so not with contextcommand.)

// Initialize cache directory.
// TODO move this to a central location.
func init() {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		panic("os.UserCacheDir: " + err.Error())
	}
	prismCache = path.Join(userCacheDir, "apache_beam/prism")
	prismBinCache = path.Join(prismCache, "bin")
}

var (
	prismCache    string
	prismBinCache string
)

const (
	beamVersion  = "v2.69.0"
	tagRoot      = "https://github.com/apache/beam/releases/tag"
	downloadRoot = "https://github.com/apache/beam/releases/download"
)

func constructDownloadPath(rootTag, version string) string {
	arch := runtime.GOARCH
	opsys := runtime.GOOS

	// strip RC versions if necessary.
	if b, _, found := strings.Cut(version, "-RC"); found {
		version = b
	}

	filename := fmt.Sprintf("apache_beam-%s-prism-%s-%s.zip", version, opsys, arch)

	return fmt.Sprintf("%s/%s/%s", downloadRoot, rootTag, filename)
}

func downloadToCache(url, local string) error {
	dir := filepath.Dir(local)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, "download-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpName)
		}
	}()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to temporary file
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return err
	}

	if err := tmpFile.Close(); err != nil {
		return err
	}
	tmpFile = nil

	if err := os.Rename(tmpName, local); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	return nil
}

// unzipCachedFile extracts the output file from the zip file.
func unzipCachedFile(zipfile, outputDir string) (string, error) {
	zr, err := zip.OpenReader(zipfile)
	if err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't open file: %w", err)
	}
	defer func() {
		_ = zr.Close()
	}()

	if len(zr.File) == 0 {
		return "", fmt.Errorf("unzipCachedFile: zip archive is empty")
	}

	output := filepath.Join(outputDir, zr.File[0].Name)

	if fi, err := os.Stat(output); err == nil && fi.Size() > 0 {
		// Binary already exists, ensure executable permissions.
		_ = os.Chmod(output, 0755)
		return output, nil
	}

	br, err := zr.File[0].Open()
	if err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't open inner file: %w", err)
	}
	defer func() {
		_ = br.Close()
	}()

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", err
	}

	tmpFile, err := os.CreateTemp(outputDir, "unzip-*.tmp")
	if err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't create executable: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := io.Copy(tmpFile, br); err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't copy file to final destination: %w", err)
	}

	// Make file executable before renaming.
	if err := tmpFile.Chmod(0755); err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't make output file executable: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	tmpFile = nil

	if err := os.Rename(tmpName, output); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("unzipCachedFile: couldn't rename to final destination: %w", err)
	}

	return output, nil
}

func withCacheLock(lockDir string, fn func() error) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		err := os.Mkdir(lockDir, 0755)
		if err == nil {
			defer func() {
				_ = os.Remove(lockDir)
			}()
			return fn()
		}
		if time.Now().After(deadline) {
			// Break stale lock if expired
			_ = os.Remove(lockDir)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type Options struct {
	Location string // if specified, indicates where a prism binary or zip of the binary can be found.
	Port     string // if specified, provdies the connection port Prism should use. Otherwise uses the default port a random port.
}

// Handle provides a shared handle into a prism process.
type Handle struct {
	addr     string
	cancelFn func()
	cmd      *exec.Cmd
}

// Terminate ends the prism process.
func (h *Handle) Terminate() {
	h.cancelFn()
}

// Addr returns the determined port and host address for the prism process.
func (h *Handle) Addr() string {
	return h.addr
}

func pickPort() string {
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		lis, err2 := net.Listen("tcp", "127.0.0.1:0")
		if err2 != nil {
			panic(fmt.Errorf("couldn't select random port to listen to: %w", err2))
		}
		defer func() {
			_ = lis.Close()
		}()
		_, port, _ := net.SplitHostPort(lis.Addr().String())
		return port
	}
	defer func() {
		_ = l.Close()
	}()
	_, port, _ := net.SplitHostPort(l.LocalAddr().String())
	return port
}

var (
	cacheMu sync.Mutex
	cache   = map[Options]*Handle{}
)

type endpointDetector struct {
	mu           sync.Mutex
	endpointChan chan string
	found        bool
}

func (d *endpointDetector) Write(p []byte) (n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, _ = os.Stdout.Write(p)
	if !d.found {
		s := string(p)
		if idx := strings.Index(s, "endpoint="); idx != -1 {
			after := s[idx+len("endpoint="):]
			fields := strings.Fields(after)
			if len(fields) > 0 {
				d.found = true
				d.endpointChan <- strings.Trim(fields[0], "\"\r\n\t")
			}
		}
	}
	return len(p), nil
}

// Start downloads and begins a prism process.
//
// Returns a cancellation function to be called once the process is no
// longer needed.
func Start(ctx context.Context, opts Options) (*Handle, error) {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if h, ok := cache[opts]; ok {
		if h.cmd.ProcessState == nil {
			return h, nil
		}
		delete(cache, opts)
	}

	var bin string
	localPath := opts.Location
	if localPath == "" {
		url := constructDownloadPath(beamVersion, beamVersion)

		if err := os.MkdirAll(prismBinCache, 0755); err != nil {
			return nil, err
		}
		basename := path.Base(url)
		localPath = filepath.Join(prismBinCache, basename)
		lockDir := filepath.Join(prismCache, "download.lock")

		err := withCacheLock(lockDir, func() error {
			expectedBin := filepath.Join(prismBinCache, strings.TrimSuffix(basename, ".zip"))
			if fi, err := os.Stat(expectedBin); err == nil && fi.Size() > 0 {
				_ = os.Chmod(expectedBin, 0755)
				bin = expectedBin
				return nil
			}

			fi, err := os.Stat(localPath)
			if err != nil || fi.Size() == 0 {
				if err := downloadToCache(url, localPath); err != nil {
					return fmt.Errorf("couldn't download %v to cache %s: %w", url, localPath, err)
				}
			}

			extracted, err := unzipCachedFile(localPath, prismBinCache)
			if err != nil {
				_ = os.Remove(localPath)
				return fmt.Errorf("couldn't unzip %q: %w", localPath, err)
			}
			bin = extracted
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		if strings.HasSuffix(localPath, ".zip") {
			extracted, err := unzipCachedFile(localPath, prismBinCache)
			if err != nil {
				return nil, fmt.Errorf("couldn't unzip custom location %q: %w", localPath, err)
			}
			bin = extracted
		} else {
			if _, err := os.Stat(localPath); err != nil {
				return nil, fmt.Errorf("prism binary not found at %q: %w", localPath, err)
			}
			_ = os.Chmod(localPath, 0755)
			bin = localPath
		}
	}

	args := []string{
		"--idle_shutdown_timeout=5s",
		"--serve_http=false",
		"--log_kind=text",
	}

	port := opts.Port
	if port == "" {
		port = "0"
	}
	cmdArgs := append(slices.Clone(args), "--job_port="+port)
	cmd := exec.Command(bin, cmdArgs...)

	detector := &endpointDetector{
		endpointChan: make(chan string, 1),
	}
	cmd.Stdout = detector
	cmd.Stderr = detector

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start command %q: %w", bin, err)
	}

	var addr string
	select {
	case addr = <-detector.endpointChan:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("timed out waiting for prism endpoint on startup")
	}

	handle := &Handle{
		addr: addr,
		cancelFn: func() {
			_ = cmd.Process.Kill()
		},
		cmd: cmd,
	}
	go func() {
		state, err := cmd.Process.Wait()
		cacheMu.Lock()
		defer cacheMu.Unlock()
		if err != nil && state != nil && !state.Success() {
			fmt.Printf("command %v returned: state %v, err %v\n", cmd.Args, state, err)
		}
		delete(cache, opts)
	}()
	cache[opts] = handle
	return handle, nil
}
