# Host/Plugin IPC Architecture Guide

This document outlines how to import and use the cross-platform shared-memory ringbuffer IPC library directly from the Git repository (`github.com/snowmerak/plugins`) across **Go**, **Rust**, **Python**, and **TypeScript**.

---

## Architecture Overview

The Host/Plugin model uses a file-backed shared memory ringbuffer for fast data transport, and local Unix Domain Sockets (`AF_UNIX`) for low-latency synchronization signaling.

1. **Host Role**: Acts as the IPC Server. It clears/unlinks existing socket paths, starts listening on the signaling socket, and instantiates the shared memory buffers.
2. **Plugin Role**: Acts as the IPC Client. It connects to the signaling socket hosted by the Host, and maps the same shared memory files.
3. **Data Channels**: Once the connection handshake completes, both processes receive a dedicated `Writer` and `Reader`:
   - The **Host** writes to `basePath + "_writer"` and reads from `basePath + "_reader"`.
   - The **Plugin** writes to `basePath + "_reader"` and reads from `basePath + "_writer"`.

---

## 1. Go Usage

### Git Dependency Installation

Add the module to your `go.mod` using `go get`:

```bash
go get github.com/snowmerak/plugins/go/ringbuf@main
```

Then reference it in your code:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/snowmerak/plugins/go/ringbuf"
)

func main() {
	basePath := "/tmp/ipc_shm"
	numSlots := 8
	slotSize := 4096

	// Initialize connection (Use ringbuf.RoleHost or ringbuf.RolePlugin)
	writer, reader, err := ringbuf.NewConnection(
		basePath,
		ringbuf.RoleHost, // or ringbuf.RolePlugin
		numSlots,
		slotSize,
		15*time.Second, // Write timeout
		15*time.Second, // Read timeout
	)
	if err != nil {
		log.Fatalf("Failed to initialize connection: %v", err)
	}
	defer writer.Close()
	defer reader.Close()

	// Write payload
	payload := []byte("hello from Go")
	if _, err := writer.Write(payload); err != nil {
		log.Fatalf("Write failed: %v", err)
	}

	// Read payload
	buf := make([]byte, len(payload))
	if _, err := reader.ReadFull(buf); err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	fmt.Printf("Received: %s\n", string(buf))
}
```

---

## 2. Rust Usage

### Git Dependency Installation

Add the dependency to your `Cargo.toml` pointing to the Git repository. Cargo automatically resolves the package from the monorepo:

```toml
[dependencies]
ringbuf-rust = { git = "https://github.com/snowmerak/plugins.git", package = "ringbuf-rust" }
```

### Synchronous API

```rust
use std::io::{Read, Write};
use std::time::Duration;
use ringbuf_rust::{new_connection, ROLE_HOST, ROLE_PLUGIN};

fn main() -> std::io::Result<()> {
    let (mut writer, mut reader) = new_connection(
        "/tmp/ipc_shm",
        ROLE_HOST, // or ROLE_PLUGIN
        8,
        4096,
        Duration::from_secs(15),
        Duration::from_secs(15),
    )?;

    // Write payload (must flush to trigger signal)
    let payload = b"hello from Rust sync";
    writer.write_all(payload)?;
    writer.flush()?;

    // Read payload
    let mut buf = vec![0u8; payload.len()];
    reader.read_exact(&mut buf)?;
    println!("Received: {:?}", String::from_utf8_lossy(&buf));

    Ok(())
}
```

### Asynchronous API (Tokio)

```rust
use std::time::Duration;
use ringbuf_rust::{new_connection_async, ROLE_HOST, ROLE_PLUGIN};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (mut writer, mut reader) = new_connection_async(
        "/tmp/ipc_shm",
        ROLE_HOST, // or ROLE_PLUGIN
        8,
        4096,
        Duration::from_secs(15),
        Duration::from_secs(15),
    ).await?;

    // Write payload (auto-flushed in async implementation)
    let payload = b"hello from Rust async";
    writer.write(payload).await?;

    // Read payload
    let mut buf = vec![0u8; payload.len()];
    reader.read_exact(&mut buf).await?;
    println!("Received: {:?}", String::from_utf8_lossy(&buf));

    Ok(())
}
```

---

## 3. Python Usage

### Git Dependency Installation

Install the package directly using `pip` targeting the subdirectory in the Git repository:

```bash
pip install "git+https://github.com/snowmerak/plugins.git#egg=ringbuf_py&subdirectory=python/ringbuf_py"
```

Or declare it in your `pyproject.toml` (PEP 508 / uv format):

```toml
dependencies = [
    "ringbuf-py @ git+https://github.com/snowmerak/plugins.git#subdirectory=python/ringbuf_py"
]
```

### Synchronous API

```python
import time
from ringbuf_py import new_connection, ROLE_HOST, ROLE_PLUGIN

# Initialize connection
writer, reader = new_connection(
    base_path="/tmp/ipc_shm",
    role=ROLE_HOST, # or ROLE_PLUGIN
    num_slots=8,
    slot_data_size=4096,
    write_timeout=15.0,
    read_timeout=15.0
)

try:
    # Write payload
    payload = b"hello from Python sync"
    writer.write(payload)

    # Read payload
    buf = reader.read_full(len(payload))
    print(f"Received: {buf.decode()}")
finally:
    writer.close()
    reader.close()
```

### Asynchronous API (asyncio)

```python
import asyncio
from ringbuf_py import async_new_connection, ROLE_HOST, ROLE_PLUGIN

async def main():
    # Initialize connection
    writer, reader = await async_new_connection(
        base_path="/tmp/ipc_shm",
        role=ROLE_HOST, # or ROLE_PLUGIN
        num_slots=8,
        slot_data_size=4096,
        write_timeout=15.0,
        read_timeout=15.0
    )

    try:
        # Write payload
        payload = b"hello from Python async"
        await writer.write(payload)

        # Read payload
        buf = await reader.read_exactly(len(payload))
        print(f"Received: {buf.decode()}")
    finally:
        await writer.close()
        await reader.close()

asyncio.run(main())
```

---

## 4. TypeScript / Node.js Usage

### Git Dependency Installation

Install the package via npm using the `#path:` or `#subdirectory:` tag (supported in modern package managers like npm 9+, yarn, and pnpm):

```bash
npm install "git+https://github.com/snowmerak/plugins.git#path:typescript/ringbuf-ts"
```

Or declare it in your `package.json`:

```json
"dependencies": {
  "ringbuf-ts": "git+https://github.com/snowmerak/plugins.git#path:typescript/ringbuf-ts"
}
```

### Usage

```typescript
import { newConnection, ROLE_HOST, ROLE_PLUGIN } from 'ringbuf-ts';

async fn main() {
    // Initialize connection
    const [writer, reader] = await newConnection(
        "/tmp/ipc_shm",
        ROLE_HOST, // or ROLE_PLUGIN
        8,
        4096,
        15000, // Write timeout in ms
        15000  // Read timeout in ms
    );

    try {
        // Write payload
        const payload = Buffer.from("hello from TypeScript");
        await writer.write(payload);

        // Read payload
        const buf = await reader.readFull(payload.length);
        console.log(`Received: ${buf.toString()}`);
    } finally {
        writer.close();
        reader.close();
    }
}

main();
```
