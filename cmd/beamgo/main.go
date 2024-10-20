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

// beamgo is a convenience builder and launcher for Beam Go WASI SDK pipelines jobs.
//
// Currently a non-functional WIP.
//
// In particular it it properly configures the go toolchain to build a wasm binary
// for the WASI environment container.
//
// When targeting go code, it will invoke the go toolchain to build the worker binary.
// It then loads that binary into a wasi host to execute per the command line parameters.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
)

// Config handles configuring the launcher
type Config struct {
	// Launch
	Launch string
}

func initFlags() *Config {
	var cfg Config
	flag.StringVar(&cfg.Launch, "launch", "", "used to specify a wasm binary to execute")
	return &cfg
}

func main() {
	cfg := initFlags()
	flag.Parse()

	ctx := context.Background()

	// Compile the go binary as wasm if necessary.
	if cfg.Launch == "" {
		cfg.Launch = "pipeline.wasm"
		goCmd := exec.CommandContext(ctx, "go", "build", "-o "+cfg.Launch, "*.go")
		goCmd.Env = append(goCmd.Env, "GOOS=wasip1", "GOARCH=wasm")
		if err := goCmd.Run(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Start the local wasi host
	// TODO, remember how to do that
}
