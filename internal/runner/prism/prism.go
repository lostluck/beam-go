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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	beamVersion  = "v2.60.0"
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
	out, err := os.Create(local)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
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
	defer zr.Close()

	br, err := zr.File[0].Open()
	if err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't inner file: %w", err)
	}
	defer br.Close()

	output := path.Join(outputDir, zr.File[0].Name)

	if _, err := os.Stat(output); err == nil {
		// Binary already exists, lets assume it's the right one.
		return output, nil
	}

	out, err := os.Create(output)
	if err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't create executable: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, br); err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't copy file to final destination: %w", err)
	}

	// Make file executable.
	if err := out.Chmod(0777); err != nil {
		return "", fmt.Errorf("unzipCachedFile: couldn't make output file executable: %w", err)
	}
	return output, nil
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
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Errorf("couldn't select random port to listen to: %w", err))
	}
	defer lis.Close()
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return port
}

var (
	cacheMu sync.Mutex
	cache   = map[Options]*Handle{}
)

// Start downloads and begins a prism process.
//
// Returns a cancellation function to be called once the process is no
// longer needed.
func Start(ctx context.Context, opts Options) (*Handle, error) {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if h, ok := cache[opts]; ok {
		// Probably not safe, but
		if h.cmd.ProcessState == nil {
			return h, nil
		}
		delete(cache, opts)
	}

	localPath := opts.Location
	if localPath == "" {
		url := constructDownloadPath(beamVersion, beamVersion)

		// Ensure the cache exists.
		if err := os.MkdirAll(prismBinCache, 0777); err != nil {
			return nil, err
		}
		basename := path.Base(url)
		localPath = filepath.Join(prismBinCache, basename)
		// Check if the zip is already in the cache.
		if _, err := os.Stat(localPath); err != nil {
			// Assume the file doesn't exist.
			if err := downloadToCache(url, localPath); err != nil {
				return nil, fmt.Errorf("couldn't download %v to cache %s: %w", url, localPath, err)
			}
		}
	}
	bin, err := unzipCachedFile(localPath, prismBinCache)
	if err != nil {
		if !errors.Is(err, zip.ErrFormat) {
			return nil, fmt.Errorf("couldn't unzip %q: %w", localPath, err)
		}
		// If it's a format error, assume it's an executable.
		bin = localPath
	}
	args := []string{
		"--idle_shutdown_timeout=1s",
		"--serve_http=false",
	}
	port := opts.Port
	if port == "" {
		port = pickPort()
	}
	args = append(args, "--job_port="+port)

	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start command %q: %w", bin, err)
	}
	handle := &Handle{
		addr: "localhost:" + port,
		cancelFn: func() {
			cmd.Process.Kill()
		},
		cmd: cmd,
	}
	go func() {
		state, err := cmd.Process.Wait()
		cacheMu.Lock()
		defer cacheMu.Unlock()
		if err != nil {
			fmt.Println("command returned: state %v, err %v", state, err)
		}
		delete(cache, opts)
	}()
	cache[opts] = handle
	return handle, nil
}
