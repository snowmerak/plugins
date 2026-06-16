# plugins

A cross-platform, high-performance memory-mapped shared memory ringbuffer package written in Go for host-plugin communications.

## Features

- **Cross-Platform**: Automatically uses platform-native memory mapping operations.
  - **Windows**: Implemented using Win32 API functions (`CreateFileMapping`, `MapViewOfFile`, `UnmapViewOfFile`, `CloseHandle`).
  - **Linux/Unix**: Implemented using POSIX `mmap` and `munmap`.
- **Lock-Free Slot Headers**: Slot metadata uses atomic state operations for low latency.
- **IPC Signaling**: Utilizes local Unix Domain Sockets (`net.Conn`) to signal state changes between host and plugin processes (fully supported natively since Windows 10 Build 17063).

## Directory Structure

```
├── ringbuf/
│   ├── ringbuf.go          # Core logic, Reader/Writer implementation, and Connection setup
│   ├── ringbuf_unix.go     # POSIX mmap implementation (build tagged for !windows)
│   ├── ringbuf_windows.go  # Win32 file mapping implementation (build tagged for windows)
│   └── ringbuf_test.go     # Test suite & benchmarks
├── ringbuf-rust/           # Rust implementation of the ringbuffer
│   ├── Cargo.toml          # Cargo dependencies (memmap2, uds_windows)
│   └── src/
│       ├── lib.rs          # Rust ringbuffer core logic (Writer/Reader/Atomic layout)
│       └── main.rs         # Rust demo binary (Host/Plugin role handler)
├── ringbuf-go-demo/
│   └── main.go             # Go demo binary (Host/Plugin role handler)
├── go.mod
└── README.md
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

## Running Tests & Benchmarks

### Windows (Natively)

```bash
# Run tests
go test -v ./ringbuf

# Run benchmarks
go test -bench=BenchmarkConnection_WriteRead -benchmem ./ringbuf
```

### Linux (WSL)

```bash
# Run tests
wsl /home/linuxbrew/.linuxbrew/bin/go test -v ./ringbuf

# Run benchmarks
wsl /home/linuxbrew/.linuxbrew/bin/go test -bench=BenchmarkConnection_WriteRead -benchmem ./ringbuf
```

## Go <-> Rust Cross-Language IPC Demo

We provide demo binaries in both Go and Rust to verify real-time, cross-language communication via the memory-mapped ring buffer and UNIX domain sockets.

### Run on Windows (Natively)

Open two terminal windows from the root directory:

**Terminal 1 (Go Host):**
```bash
go run ringbuf-go-demo/main.go --role Host --path temp_shm
```

**Terminal 2 (Rust Plugin):**
```bash
cargo run --manifest-path ringbuf-rust/Cargo.toml -- --role Plugin --path temp_shm
```

### Run on Linux (WSL)

Open two terminal windows from the root directory:

**Terminal 1 (Go Host):**
```bash
wsl /home/linuxbrew/.linuxbrew/bin/go run ringbuf-go-demo/main.go --role Host --path /tmp/temp_shm
```

**Terminal 2 (Rust Plugin):**
```bash
wsl /home/merak/.rustup/toolchains/stable-aarch64-unknown-linux-gnu/bin/cargo run --manifest-path ringbuf-rust/Cargo.toml -- --role Plugin --path /tmp/temp_shm
```

