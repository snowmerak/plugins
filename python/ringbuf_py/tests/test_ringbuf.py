import socket
import threading
import sys
import time
import pytest
from ringbuf_py import RingBuffer, Writer, Reader, new_connection, ROLE_HOST, ROLE_PLUGIN

def local_socket_pair():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    conn1 = None
    err = None

    def accept_thread():
        nonlocal conn1, err
        try:
            conn1, _ = listener.accept()
        except Exception as e:
            err = e

    t = threading.Thread(target=accept_thread)
    t.start()

    conn2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn2.connect(("127.0.0.1", port))
    t.join()
    listener.close()

    if err is not None:
        raise err

    return conn1, conn2

def get_test_base_path(tmp_path):
    if sys.platform != "win32" and str(tmp_path).startswith("/mnt/"):
        import uuid
        return f"/tmp/shm_test_{uuid.uuid4().hex}"
    return str(tmp_path / "shm_comm")

def test_ringbuffer_basic(tmp_path):
    file_path = str(tmp_path / "shm_file")
    rb = RingBuffer(file_path, 2, 64)
    rb.clear()

    c1, c2 = local_socket_pair()
    try:
        # Write first slot
        payload1 = b"hello world"
        rb.write(payload1, 42, 0.01, c1)

        # Read first slot
        read_payload1, seq1 = rb.read(0.01, c2)

        assert seq1 == 42
        assert read_payload1 == payload1
    finally:
        c1.close()
        c2.close()
        rb.close()

def test_ringbuffer_timeout_and_drop(tmp_path):
    file_path = str(tmp_path / "shm_file")
    rb = RingBuffer(file_path, 2, 64)
    rb.clear()

    c1, c2 = local_socket_pair()
    try:
        # Write 1st slot (index 0)
        rb.write(b"msg1", 1, 0.01, c1)
        # Write 2nd slot (index 1)
        rb.write(b"msg2", 2, 0.01, c1)

        # Buffer is now full. Next write should wait on slot 0 and timeout because slot 0 is not read yet.
        with pytest.raises(TimeoutError):
            rb.write(b"msg3", 3, 0.02, c1)

        # The index should have advanced to slot 1 due to the drop/skip
        assert rb.write_index == 1
    finally:
        c1.close()
        c2.close()
        rb.close()

@pytest.mark.skipif(
    not hasattr(socket, "AF_UNIX"),
    reason="UNIX domain sockets (AF_UNIX) are not supported on this platform (e.g. Windows)"
)
def test_connection_basic(tmp_path):
    base_path = get_test_base_path(tmp_path)

    host_writer = None
    host_reader = None
    plugin_writer = None
    plugin_reader = None

    class HostResult:
        def __init__(self):
            self.writer = None
            self.reader = None
            self.err = None

    host_res = HostResult()

    def host_thread():
        try:
            w, r = new_connection(
                base_path=base_path,
                role=ROLE_HOST,
                num_slots=2,
                slot_data_size=64,
                write_timeout=0.5,
                read_timeout=0.5,
            )
            host_res.writer = w
            host_res.reader = r
        except Exception as e:
            host_res.err = e

    t = threading.Thread(target=host_thread)
    t.start()

    try:
        plugin_writer, plugin_reader = new_connection(
            base_path=base_path,
            role=ROLE_PLUGIN,
            num_slots=2,
            slot_data_size=64,
            write_timeout=0.5,
            read_timeout=0.5,
        )
    except Exception as e:
        t.join()
        raise e

    t.join()
    if host_res.err is not None:
        plugin_writer.close()
        plugin_reader.close()
        raise host_res.err

    host_writer = host_res.writer
    host_reader = host_res.reader

    try:
        # Host writes, Plugin reads
        msg_from_host = b"hello from host"
        host_writer.write(msg_from_host)

        read_buf_plugin = plugin_reader.read(100)
        assert read_buf_plugin == msg_from_host

        # Plugin writes, Host reads
        msg_from_plugin = b"hello from plugin"
        plugin_writer.write(msg_from_plugin)

        read_buf_host = host_reader.read(100)
        assert read_buf_host == msg_from_plugin

    finally:
        if host_writer:
            host_writer.close()
        if host_reader:
            host_reader.close()
        if plugin_writer:
            plugin_writer.close()
        if plugin_reader:
            plugin_reader.close()

        # Clean up files created during connection tests
        import os
        for ext in ["_sig.sock", "_writer", "_reader"]:
            path = base_path + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

def test_large_data_integrity(tmp_path):
    file_path = str(tmp_path / "shm_file_large")
    rb = RingBuffer(file_path, 2, 64)
    rb.clear()

    c1, c2 = local_socket_pair()
    
    writer = Writer(rb, c1, 1.0)
    reader = Reader(rb, c2, 1.0)

    # 250 bytes payload. With 64-byte slots, it requires 4 chunks:
    # Chunk 1 (64), Chunk 2 (64), Chunk 3 (64), Chunk 4 (58)
    large_payload = bytes([i % 256 for i in range(250)])

    read_result = bytearray()
    read_err = None

    def read_thread():
        nonlocal read_err
        try:
            res = reader.read_full(250)
            read_result.extend(res)
        except Exception as e:
            read_err = e

    t = threading.Thread(target=read_thread)
    t.start()

    try:
        written = writer.write(large_payload)
        assert written == 250
    finally:
        t.join()
        writer.close()
        reader.close()

    if read_err is not None:
        raise read_err

    assert bytes(read_result) == large_payload

