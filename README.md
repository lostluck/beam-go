# beam-go

[![Go Reference](https://pkg.go.dev/badge/lostluck.dev/beam-go.svg)](https://pkg.go.dev/lostluck.dev/beam-go) [![license](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://raw.githubusercontent.com/lostluck/beam-go/master/LICENSE) [![Build](https://github.com/lostluck/beam-go/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/lostluck/beam-go/actions/workflows/go.yml) [![codecov](https://codecov.io/gh/lostluck/beam-go/graph/badge.svg?token=SC0CK4DMAM)](https://codecov.io/gh/lostluck/beam-go) [![Go Report Card](https://goreportcard.com/badge/lostluck.dev/beam-go)](https://goreportcard.com/report/lostluck.dev/beam-go)

An Unofficial Alternative Apache Beam Go SDK

## Status: Experimental

Not currently fit for production use, but ready for experimentation and feedback.
Breaking changes are currently possible and likely.

However, pipelines should execute correctly on any runner that has a Beam 
JobManagement service endpoint.

# Compatibility and Differences

Notably, it's using a completely different approach to building and executing
the pipeline, in order to allow Go to typecheck the pipeline, instead of
reflection heavy SDK side code. As a result it's not currently possible to
mix and match versions. In particular, this is actively prevented at
program initialization time via a protocol buffer collision.

Things that are different from the original Apache Beam Go SDK.

   - Coders
   - DoFns
   - Use of Beam featues
   - Pipeline Construction
   - No registration required.
   - Re-builds pipelines on worker initialization
   - Avoids package level variables
   - Tighter user surface

# TODOs

- Fix Pipeline handle ergonomics regarding extracting/querying metrics and status.
- WindowInto, Triggers and Panes
  - PAssert
- UnboundedSDFs and ProcessContinuations
- State and Timers
- User Defined Coders
  - Simple Compression Coders?
- Additional examples and Documentation
- Submitting jobs to Google Cloud Dataflow

## Things to Consider

* Split into multiple purposeful packages: One for Pipeline Construction, and one for DoFn definition?
  * Would that make implementation too difficult? Would we just be wrapping a
    single "internal" package with two different user packages? Would that make
    the GoDoc easier to parse for users?
  * Would "job launching" be a 3rd separate package for operational clarity?
  * Would just having robustly documented sections, and unambiguous documentation
    in the main beam package be sufficient?
  * Would that break pipeline construction with beam.PCol doing double duty?
