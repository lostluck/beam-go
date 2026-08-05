# Gemini Workspace Context

This document provides context for the `beam-go` project to an AI assistant.

## Project Description

`beam-go` is an unofficial, experimental alternative to the official Apache Beam Go SDK. The primary goal of this project is to provide a more type-safe and idiomatic Go experience for building and executing Beam pipelines. It avoids the reflection-heavy approach of the official SDK, instead opting for a design that allows the Go compiler to perform more static analysis and type checking of the pipeline code.

Key differences from the official Apache Beam Go SDK include:
-   **Type Safety:** Leverages Go's type system to validate pipelines at compile time, rather than at runtime.
-   **No Registration:** Does not require manual registration of DoFns or types.
-   **Simplified API:** Aims for a tighter, more user-friendly public API surface.
-   **Pipeline Construction:** Uses a different approach for pipeline graph construction.

The project is currently experimental and not suitable for production use.

## Development Environment

The project is written in Go.

### Build Instructions

To build the project, run the following command from the root directory:
```sh
go build -v ./...
```

### Test Instructions

To run the test suite, use the following command:
```sh
go test -v -cover ./...
```

The project uses `codecov` for code coverage reporting.
