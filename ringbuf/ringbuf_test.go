package ringbuf

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// localSocketPair creates a connected pair of local TCP sockets
// to act as high-fidelity simulated signal connections for unit tests.
func localSocketPair(t *testing.T) (net.Conn, net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	var c1 net.Conn
	var err1 error
	done := make(chan struct{})
	go func() {
		c1, err1 = ln.Accept()
		close(done)
	}()

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	<-done
	if err1 != nil {
		c2.Close()
		t.Fatalf("failed to accept: %v", err1)
	}
	return c1, c2
}

func TestRingBuffer_Basic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "shm_file")
	rb, err := newRingBuffer(filePath, 2, 64)
	if err != nil {
		t.Fatalf("failed to create ringBuffer: %v", err)
	}
	defer rb.Close()

	rb.Clear()

	c1, c2 := localSocketPair(t)
	defer c1.Close()
	defer c2.Close()

	// Write first slot
	payload1 := []byte("hello world")
	if err := rb.Write(payload1, 42, 10*time.Millisecond, c1); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read first slot
	readPayload1, seq1, err := rb.Read(10*time.Millisecond, c2)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if seq1 != 42 {
		t.Errorf("expected seq 42, got %d", seq1)
	}
	if !bytes.Equal(readPayload1, payload1) {
		t.Errorf("expected %s, got %s", payload1, readPayload1)
	}
}

func TestRingBuffer_TimeoutAndDrop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "shm_file")
	// Buffer has 2 slots, size 64 each
	rb, err := newRingBuffer(filePath, 2, 64)
	if err != nil {
		t.Fatalf("failed to create ringBuffer: %v", err)
	}
	defer rb.Close()

	rb.Clear()

	c1, c2 := localSocketPair(t)
	defer c1.Close()
	defer c2.Close()

	// Write 1st slot (index 0)
	if err := rb.Write([]byte("msg1"), 1, 10*time.Millisecond, c1); err != nil {
		t.Fatalf("failed to write msg1: %v", err)
	}

	// Write 2nd slot (index 1)
	if err := rb.Write([]byte("msg2"), 2, 10*time.Millisecond, c1); err != nil {
		t.Fatalf("failed to write msg2: %v", err)
	}

	// Buffer is now full.
	// Next write should wait on slot 0 and timeout because slot 0 is not read yet.
	err = rb.Write([]byte("msg3"), 3, 20*time.Millisecond, c1)
	if err == nil {
		t.Fatal("expected write timeout/drop error, got nil")
	}

	if !strings.Contains(err.Error(), "write timeout") {
		t.Errorf("unexpected error message: %v", err)
	}

	// The index should have advanced to slot 1 due to the drop/skip
	if rb.GetWriteIndex() != 1 {
		t.Errorf("expected write index to advance to 1, got %d", rb.GetWriteIndex())
	}
}

func TestConnection_Basic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "shm_comm")

	type connResult struct {
		writer *Writer
		reader *Reader
		err    error
	}

	// Create Host connection asynchronously
	hostChan := make(chan connResult, 1)
	go func() {
		w, r, err := NewConnection(basePath, RoleHost, 2, 64, 500*time.Millisecond, 500*time.Millisecond)
		hostChan <- connResult{w, r, err}
	}()

	// Create Plugin connection synchronously
	pluginWriter, pluginReader, err := NewConnection(basePath, RolePlugin, 2, 64, 500*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create Plugin connection: %v", err)
	}
	defer pluginWriter.Close()
	defer pluginReader.Close()

	res := <-hostChan
	if res.err != nil {
		t.Fatalf("failed to create Host connection: %v", res.err)
	}
	hostWriter := res.writer
	hostReader := res.reader
	defer hostWriter.Close()
	defer hostReader.Close()

	// Host writes, Plugin reads
	msgFromHost := []byte("hello from host")
	_, err = hostWriter.Write(msgFromHost)
	if err != nil {
		t.Fatalf("failed to write from host: %v", err)
	}

	readBufPlugin := make([]byte, 100)
	rn, err := pluginReader.Read(readBufPlugin)
	if err != nil {
		t.Fatalf("plugin failed to read: %v", err)
	}
	if !bytes.Equal(readBufPlugin[:rn], msgFromHost) {
		t.Errorf("expected %s, got %s", msgFromHost, readBufPlugin[:rn])
	}

	// Plugin writes, Host reads
	msgFromPlugin := []byte("hello from plugin")
	_, err = pluginWriter.Write(msgFromPlugin)
	if err != nil {
		t.Fatalf("failed to write from plugin: %v", err)
	}

	readBufHost := make([]byte, 100)
	rn, err = hostReader.Read(readBufHost)
	if err != nil {
		t.Fatalf("host failed to read: %v", err)
	}
	if !bytes.Equal(readBufHost[:rn], msgFromPlugin) {
		t.Errorf("expected %s, got %s", msgFromPlugin, readBufHost[:rn])
	}
}

func TestConnection_MultiSlot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "shm_comm")

	type connResult struct {
		writer *Writer
		reader *Reader
		err    error
	}

	// Host connection (4 slots, 64 bytes payload each) asynchronously
	hostChan := make(chan connResult, 1)
	go func() {
		w, r, err := NewConnection(basePath, RoleHost, 4, 64, 500*time.Millisecond, 500*time.Millisecond)
		hostChan <- connResult{w, r, err}
	}()

	// Plugin connection synchronously
	pluginWriter, pluginReader, err := NewConnection(basePath, RolePlugin, 4, 64, 500*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create Plugin connection: %v", err)
	}
	defer pluginWriter.Close()
	defer pluginReader.Close()

	res := <-hostChan
	if res.err != nil {
		t.Fatalf("failed to create Host connection: %v", res.err)
	}
	hostWriter := res.writer
	hostReader := res.reader
	defer hostWriter.Close()
	defer hostReader.Close()

	// Host writes 150 bytes, spanning 3 slots
	payload := make([]byte, 150)
	for i := range payload {
		payload[i] = byte(i)
	}

	n, err := hostWriter.Write(payload)
	if err != nil {
		t.Fatalf("failed to write multi-slot payload: %v", err)
	}
	if n != len(payload) {
		t.Errorf("expected %d bytes written, got %d", len(payload), n)
	}

	// Plugin reads it back fully
	readBuf := make([]byte, 150)
	_, err = pluginReader.ReadFull(readBuf)
	if err != nil {
		t.Fatalf("failed to read multi-slot payload: %v", err)
	}

	if !bytes.Equal(readBuf, payload) {
		t.Error("read payload does not match written payload")
	}
}

func BenchmarkConnection_WriteRead(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "shm_bench_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "shm_bench")

	type connResult struct {
		writer *Writer
		reader *Reader
		err    error
	}

	hostChan := make(chan connResult, 1)
	go func() {
		w, r, err := NewConnection(basePath, RoleHost, 8, 4096, 500*time.Millisecond, 500*time.Millisecond)
		hostChan <- connResult{w, r, err}
	}()

	pluginWriter, pluginReader, err := NewConnection(basePath, RolePlugin, 8, 4096, 500*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		b.Fatalf("failed to create Plugin connection: %v", err)
	}
	defer pluginWriter.Close()
	defer pluginReader.Close()

	res := <-hostChan
	if res.err != nil {
		b.Fatalf("failed to create Host connection: %v", res.err)
	}
	hostWriter := res.writer
	hostReader := res.reader
	defer hostWriter.Close()
	defer hostReader.Close()

	payload := make([]byte, 1024) // 1KB message
	readBuf := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = hostWriter.Write(payload)
		if err != nil {
			b.Fatalf("write failed: %v", err)
		}
		_, err = pluginReader.ReadFull(readBuf)
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
}
