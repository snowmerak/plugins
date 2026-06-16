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
├── go.work                 # Go workspace definition
├── Taskfile.yml            # Task orchestrator configuration
├── .gitignore              # Monorepo ignore rules
└── README.md
```

## Running Tasks (via Taskfile)

We use `task` (Taskfile) to manage test and run commands across Go, Rust, and Python.

### Windows (Natively)

```bash
# Run Go unit tests
task go:test

# Run Rust workspace build
task rust:build

# Run Python unit tests (Windows skips UNIX socket tests)
task py:test

# Run Go Host Demo
task demo:host-go

# Run Rust Plugin Demo
task demo:plugin-rust

# Run Python Plugin Demo (Note: Windows lacks native AF_UNIX support in Python)
task demo:plugin-py
```

### Linux (WSL)

```bash
# Run Go unit tests inside WSL
task go:test-wsl

# Run Rust workspace build inside WSL
task rust:build-wsl

# Run Python unit tests inside WSL (runs full test suite)
task py:test-wsl

# Run Go Host Demo inside WSL
task demo:host-go-wsl

# Run Rust Plugin Demo inside WSL
task demo:plugin-rust-wsl

# Run Python Plugin Demo inside WSL
task demo:plugin-py-wsl
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
