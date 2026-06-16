package ringbuf

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	StateEmpty   uint32 = 0
	StateWritten uint32 = 1
)

const (
	RoleHost   = "Host"
	RolePlugin = "Plugin"
)

type ringBuffer struct {
	data         []byte
	numSlots     int
	slotSize     int
	slotDataSize int
	writeIndex   int
	readIndex    int
	sysMapping   interface{}
}

// newRingBuffer creates or opens a file-backed mmap ring buffer and closes the file descriptor immediately.
func newRingBuffer(filePath string, numSlots int, slotDataSize int) (*ringBuffer, error) {
	if numSlots <= 0 || slotDataSize <= 0 {
		return nil, errors.New("invalid dimensions")
	}

	// Slot layout:
	// 0..3: flag (uint32)
	// 4..7: len (uint32)
	// 8..15: seq (uint64)
	// 16..19: writerWaiting (uint32)
	// 20..23: readerWaiting (uint32)
	// 24..: payload (slotDataSize)
	slotSize := 24 + slotDataSize
	totalSize := numSlots * slotSize

	data, sysMapping, err := mmapFile(filePath, totalSize)
	if err != nil {
		return nil, err
	}

	return &ringBuffer{
		data:         data,
		numSlots:     numSlots,
		slotSize:     slotSize,
		slotDataSize: slotDataSize,
		writeIndex:   0,
		readIndex:    0,
		sysMapping:   sysMapping,
	}, nil
}

// Close unmaps the memory mapped region.
func (rb *ringBuffer) Close() error {
	if rb.data != nil {
		if err := unmapFile(rb.data, rb.sysMapping); err != nil {
			return fmt.Errorf("failed to unmap: %w", err)
		}
		rb.data = nil
	}
	return nil
}

// Clear resets the header flags of all slots to StateEmpty.
func (rb *ringBuffer) Clear() {
	for i := 0; i < rb.numSlots; i++ {
		atomic.StoreUint32(rb.getFlagPtr(i), StateEmpty)
		atomic.StoreUint32(rb.getLenPtr(i), 0)
		atomic.StoreUint64(rb.getSeqPtr(i), 0)
		atomic.StoreUint32(rb.getWriterWaitingPtr(i), 0)
		atomic.StoreUint32(rb.getReaderWaitingPtr(i), 0)
	}
	rb.writeIndex = 0
	rb.readIndex = 0
}

// Write writes payload and sequence to the current slot if empty.
// If not empty, it blocks on UDS connection read waiting for an empty slot signal.
func (rb *ringBuffer) Write(payload []byte, seq uint64, timeout time.Duration, sigConn net.Conn) error {
	if len(payload) > rb.slotDataSize {
		return fmt.Errorf("payload size %d exceeds slot capacity %d", len(payload), rb.slotDataSize)
	}

	flagPtr := rb.getFlagPtr(rb.writeIndex)
	writerWaitingPtr := rb.getWriterWaitingPtr(rb.writeIndex)
	start := time.Now()

	for atomic.LoadUint32(flagPtr) != StateEmpty {
		// Mark that this writer is waiting on this slot
		atomic.StoreUint32(writerWaitingPtr, 1)

		// Double-check after setting waiting flag to prevent race condition (lost wakeup)
		if atomic.LoadUint32(flagPtr) == StateEmpty {
			atomic.StoreUint32(writerWaitingPtr, 0)
			break
		}

		if timeout > 0 {
			sigConn.SetReadDeadline(time.Now().Add(timeout - time.Since(start)))
		} else {
			sigConn.SetReadDeadline(time.Time{})
		}
		var signalBuf [1]byte
		_, err := sigConn.Read(signalBuf[:])
		
		atomic.StoreUint32(writerWaitingPtr, 0)

		if err != nil {
			idx := rb.writeIndex
			rb.writeIndex = (rb.writeIndex + 1) % rb.numSlots
			return fmt.Errorf("write timeout on slot %d waiting for empty signal: %w", idx, err)
		}
	}

	// Copy payload
	dest := rb.getPayloadSlice(rb.writeIndex)
	copy(dest, payload)

	// Store length and sequence
	atomic.StoreUint32(rb.getLenPtr(rb.writeIndex), uint32(len(payload)))
	atomic.StoreUint64(rb.getSeqPtr(rb.writeIndex), seq)

	// Atomically mark as written
	atomic.StoreUint32(flagPtr, StateWritten)

	// Signal the reader if they are waiting
	readerWaitingPtr := rb.getReaderWaitingPtr(rb.writeIndex)
	if atomic.CompareAndSwapUint32(readerWaitingPtr, 1, 0) {
		var token = [1]byte{0x01}
		sigConn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		sigConn.Write(token[:])
	}

	rb.writeIndex = (rb.writeIndex + 1) % rb.numSlots
	return nil
}

// Read waits for the current slot to be written.
// If empty, it blocks on UDS connection read waiting for a written slot signal.
func (rb *ringBuffer) Read(timeout time.Duration, sigConn net.Conn) ([]byte, uint64, error) {
	flagPtr := rb.getFlagPtr(rb.readIndex)
	readerWaitingPtr := rb.getReaderWaitingPtr(rb.readIndex)
	start := time.Now()

	for atomic.LoadUint32(flagPtr) != StateWritten {
		// Mark that this reader is waiting on this slot
		atomic.StoreUint32(readerWaitingPtr, 1)

		// Double-check after setting waiting flag to prevent race condition (lost wakeup)
		if atomic.LoadUint32(flagPtr) == StateWritten {
			atomic.StoreUint32(readerWaitingPtr, 0)
			break
		}

		if timeout > 0 {
			sigConn.SetReadDeadline(time.Now().Add(timeout - time.Since(start)))
		} else {
			sigConn.SetReadDeadline(time.Time{})
		}
		var signalBuf [1]byte
		_, err := sigConn.Read(signalBuf[:])

		atomic.StoreUint32(readerWaitingPtr, 0)

		if err != nil {
			return nil, 0, fmt.Errorf("read timeout on slot %d waiting for write signal: %w", rb.readIndex, err)
		}
	}

	length := atomic.LoadUint32(rb.getLenPtr(rb.readIndex))
	seq := atomic.LoadUint64(rb.getSeqPtr(rb.readIndex))

	if int(length) > rb.slotDataSize {
		return nil, 0, fmt.Errorf("invalid payload length %d on slot %d", length, rb.readIndex)
	}

	payload := make([]byte, length)
	src := rb.getPayloadSlice(rb.readIndex)
	copy(payload, src[:length])

	// Mark as read/empty
	atomic.StoreUint32(flagPtr, StateEmpty)

	// Signal the writer if they are waiting
	writerWaitingPtr := rb.getWriterWaitingPtr(rb.readIndex)
	if atomic.CompareAndSwapUint32(writerWaitingPtr, 1, 0) {
		var token = [1]byte{0x02}
		sigConn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		sigConn.Write(token[:])
	}

	rb.readIndex = (rb.readIndex + 1) % rb.numSlots
	return payload, seq, nil
}

// GetWriteIndex returns the current write index of the buffer slot being accessed.
func (rb *ringBuffer) GetWriteIndex() int {
	return rb.writeIndex
}

// GetReadIndex returns the current read index of the buffer slot being accessed.
func (rb *ringBuffer) GetReadIndex() int {
	return rb.readIndex
}

func (rb *ringBuffer) getFlagPtr(slot int) *uint32 {
	offset := slot * rb.slotSize
	return (*uint32)(unsafe.Pointer(&rb.data[offset]))
}

func (rb *ringBuffer) getLenPtr(slot int) *uint32 {
	offset := slot * rb.slotSize
	return (*uint32)(unsafe.Pointer(&rb.data[offset+4]))
}

func (rb *ringBuffer) getSeqPtr(slot int) *uint64 {
	offset := slot * rb.slotSize
	return (*uint64)(unsafe.Pointer(&rb.data[offset+8]))
}

func (rb *ringBuffer) getWriterWaitingPtr(slot int) *uint32 {
	offset := slot * rb.slotSize
	return (*uint32)(unsafe.Pointer(&rb.data[offset+16]))
}

func (rb *ringBuffer) getReaderWaitingPtr(slot int) *uint32 {
	offset := slot * rb.slotSize
	return (*uint32)(unsafe.Pointer(&rb.data[offset+20]))
}

func (rb *ringBuffer) getPayloadSlice(slot int) []byte {
	offset := slot * rb.slotSize
	return rb.data[offset+24 : offset+24+rb.slotDataSize]
}

type Writer struct {
	rb           *ringBuffer
	sigConn      net.Conn
	writeTimeout time.Duration
	nextWriteSeq uint64
	writeMu      sync.Mutex
}

// Write implements io.Writer. It partitions p into slots and writes them.
func (w *Writer) Write(p []byte) (n int, err error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	total := len(p)
	written := 0

	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > w.rb.slotDataSize {
			chunkSize = w.rb.slotDataSize
		}

		err = w.rb.Write(p[:chunkSize], w.nextWriteSeq, w.writeTimeout, w.sigConn)
		if err != nil {
			return written, err
		}

		w.nextWriteSeq++
		written += chunkSize
		p = p[chunkSize:]
	}

	return total, nil
}

// Close closes the signaling connection and unmaps the writer memory.
func (w *Writer) Close() error {
	var errs []error
	if w.sigConn != nil {
		if err := w.sigConn.Close(); err != nil {
			errs = append(errs, err)
		}
		w.sigConn = nil
	}
	if w.rb != nil {
		if err := w.rb.Close(); err != nil {
			errs = append(errs, err)
		}
		w.rb = nil
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

type Reader struct {
	rb          *ringBuffer
	sigConn     net.Conn
	readTimeout time.Duration
	readBuf     []byte
	readMu      sync.Mutex
}

// Read implements io.Reader. It reads next slot when internal buffer is empty.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()

	if len(p) == 0 {
		return 0, nil
	}

	if len(r.readBuf) == 0 {
		payload, _, err := r.rb.Read(r.readTimeout, r.sigConn)
		if err != nil {
			return 0, err
		}
		r.readBuf = payload
	}

	n = copy(p, r.readBuf)
	r.readBuf = r.readBuf[n:]
	return n, nil
}

// ReadFull reads exactly len(buf) bytes from the Reader into buf.
// It blocks until all bytes are read, or an error/timeout occurs.
func (r *Reader) ReadFull(buf []byte) (n int, err error) {
	return io.ReadFull(r, buf)
}

// Close closes the signaling connection and unmaps the reader memory.
func (r *Reader) Close() error {
	var errs []error
	if r.sigConn != nil {
		if err := r.sigConn.Close(); err != nil {
			errs = append(errs, err)
		}
		r.sigConn = nil
	}
	if r.rb != nil {
		if err := r.rb.Close(); err != nil {
			errs = append(errs, err)
		}
		r.rb = nil
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// NewConnection creates the Writer and Reader pair using the base filepath and node role.
// It also establishes UDS signaling connections over `{basePath}_sig.sock`.
func NewConnection(basePath string, role string, numSlots, slotDataSize int, writeTimeout, readTimeout time.Duration) (*Writer, *Reader, error) {
	var writePath, readPath string
	sigSockPath := basePath + "_sig.sock"

	var conn1, conn2 net.Conn

	if role == RoleHost {
		writePath = basePath + "_writer"
		readPath = basePath + "_reader"

		_ = os.Remove(sigSockPath)
		ln, err := net.Listen("unix", sigSockPath)
		if err != nil {
			return nil, nil, fmt.Errorf("host failed to listen on signaling socket: %w", err)
		}
		defer ln.Close()

		// Accept connection 1: Host Writer <-> Plugin Reader
		conn1, err = ln.Accept()
		if err != nil {
			return nil, nil, fmt.Errorf("host failed to accept first sig connection: %w", err)
		}

		// Accept connection 2: Plugin Writer <-> Host Reader
		conn2, err = ln.Accept()
		if err != nil {
			conn1.Close()
			return nil, nil, fmt.Errorf("host failed to accept second sig connection: %w", err)
		}

	} else if role == RolePlugin {
		writePath = basePath + "_reader"
		readPath = basePath + "_writer"

		var err error
		// Dial connection 1: Host Writer <-> Plugin Reader
		for attempt := 0; attempt < 100; attempt++ {
			conn1, err = net.Dial("unix", sigSockPath)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("plugin failed to dial first sig connection: %w", err)
		}

		// Dial connection 2: Plugin Writer <-> Host Reader
		for attempt := 0; attempt < 100; attempt++ {
			conn2, err = net.Dial("unix", sigSockPath)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			conn1.Close()
			return nil, nil, fmt.Errorf("plugin failed to dial second sig connection: %w", err)
		}

	} else {
		return nil, nil, fmt.Errorf("invalid role: %s, must be 'Host' or 'Plugin'", role)
	}

	writeRB, err := newRingBuffer(writePath, numSlots, slotDataSize)
	if err != nil {
		conn1.Close()
		conn2.Close()
		return nil, nil, err
	}
	writeRB.Clear() // Clear write flags for a fresh start

	readRB, err := newRingBuffer(readPath, numSlots, slotDataSize)
	if err != nil {
		writeRB.Close()
		conn1.Close()
		conn2.Close()
		return nil, nil, err
	}

	var writer *Writer
	var reader *Reader

	if role == RoleHost {
		writer = &Writer{
			rb:           writeRB,
			sigConn:      conn1, // Host Writer uses conn1 to signal Plugin Reader
			writeTimeout: writeTimeout,
		}
		reader = &Reader{
			rb:          readRB,
			sigConn:     conn2, // Host Reader uses conn2 to read Plugin Writer signals
			readTimeout: readTimeout,
		}
	} else {
		// role == RolePlugin
		writer = &Writer{
			rb:           writeRB,
			sigConn:      conn2, // Plugin Writer uses conn2 to signal Host Reader
			writeTimeout: writeTimeout,
		}
		reader = &Reader{
			rb:          readRB,
			sigConn:     conn1, // Plugin Reader uses conn1 to read Host Writer signals
			readTimeout: readTimeout,
		}
	}

	return writer, reader, nil
}
