# plugins

A cross-platform, high-performance memory-mapped shared memory ringbuffer package written in Go and Rust for host-plugin communications, structured as a monorepo.

## Features

- **Cross-Platform**: Automatically uses platform-native memory mapping operations.
  - **Windows**: Implemented using Win32 API functions (`CreateFileMapping`, `MapViewOfFile`, `UnmapViewOfFile`, `CloseHandle`).
  - **Linux/Unix**: Implemented using POSIX `mmap` and `munmap`.
- **Lock-Free Slot Headers**: Slot metadata uses atomic state operations for low latency.
- **IPC Signaling**: Utilizes local Unix Domain Sockets (`net.Conn`) to signal state changes between host and plugin processes (fully supported natively since Windows 10 Build 17063). Python falls back to loopback socket simulation or runs natively in WSL due to Python's platform restrictions on Windows.
- **Data Integrity & Stream Semantics**: Transparently chunks large payloads exceeding individual slot limits into sequential chunks, utilizing thread-safe mutex guards to guarantee strict sequential FIFO packet reconstruction at the receiving end without chunk interleaving.

## Directory Structure

```
├── go/
│   ├── ringbuf/            # Go ringbuffer implementation
│   │   ├── go.mod
│   │   ├── ringbuf.go
│   │   ├── ringbuf_unix.go
│   │   ├── ringbuf_windows.go
│   │   └── ringbuf_test.go
│   └── ringbuf-go-demo/    # Go demo CLI binary
│       ├── go.mod
│       └── main.go
├── rust/
│   ├── Cargo.toml          # Cargo workspace configuration
│   └── ringbuf-rust/       # Rust ringbuffer library & CLI binary
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           └── main.rs
├── python/
│   ├── pyproject.toml      # UV python workspace config
│   └── ringbuf_py/         # Python ringbuffer package
│       ├── pyproject.toml
│       ├── README.md
│       ├── ringbuf_py/     # Core Python module
│       │   ├── __init__.py
│       │   └── ringbuf.py
│       ├── ringbuf_py_demo/
│       │   └── main.py     # Python CLI demo
│       └── tests/
│           └── test_ringbuf.py # pytest unit tests
├── typescript/
│   ├── package.json        # NPM workspace configuration
│   ├── ringbuf-ts/         # TypeScript ringbuffer library
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── src/            # ringbuf.ts and index.ts
│   │   └── tests/          # Unit tests
│   └── ringbuf-ts-demo/    # TypeScript CLI demo
│       ├── package.json
│       └── src/            # main.ts
├── go.work                 # Go workspace definition
├── Taskfile.yml            # Task orchestrator configuration
├── .gitignore              # Monorepo ignore rules
└── README.md
```

## Running Tasks (via Taskfile)

We use `task` (Taskfile) to manage test, build, and run commands across Go, Rust, Python, and TypeScript.
Run these commands inside your terminal. To execute the full integration suite and UNIX socket connection tests, run them inside a UNIX environment (macOS or Linux/WSL shell) where `AF_UNIX` sockets are natively supported by all runtimes.

### Run All Unit Tests
```bash
task test
```

### Run Language-Specific Tests & Builds
```bash
# Go
task go:test

# Rust
task rust:build
task rust:test

# Python
task py:test

# TypeScript
task ts:build
task ts:test
```

### Run CLI Demos (Cross-Language IPC)
To run the demo apps, launch one language as Host and another as Plugin, pointing to the same shared memory path:

```bash
# Terminal 1: Run Go Host
go run go/ringbuf-go-demo/main.go --role Host --path /tmp/temp_shm

# Terminal 2: Run Rust Plugin
cargo run --manifest-path rust/Cargo.toml --bin ringbuf-rust -- --role Plugin --path /tmp/temp_shm
```

The Taskfile also exposes shorthand task runners:
```bash
# Go
task demo:host-go

# Rust
task demo:plugin-rust

# Python
task demo:host-py
task demo:plugin-py

# TypeScript
task demo:host-ts
task demo:plugin-ts
```

## Benchmarks

Benchmarks measure throughput for bidirectional communication using a 1KB message size over 8-slot, 4096-byte payload buffer connections.

### Windows Native (ARM64)

- **Go version**: `go1.26.4 windows/arm64`
- **Results**:
  ```
  BenchmarkConnection_WriteRead-8   3643342   307.0 ns/op   1024 B/op   1 allocs/op
  ```

### Linux WSL2 (ARM64)

- **Go version**: `go1.26.4 linux/arm64` (Debian 13)
- **Results**:
  ```
  BenchmarkConnection_WriteRead-8    765952   1375.0 ns/op   1024 B/op   1 allocs/op
  ```
