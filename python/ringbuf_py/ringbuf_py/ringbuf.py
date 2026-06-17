import os
import mmap
import struct
import socket
import time
import threading
import asyncio
from typing import Tuple

STATE_EMPTY = 0
STATE_WRITTEN = 1

ROLE_HOST = "Host"
ROLE_PLUGIN = "Plugin"

class RingBuffer:
    def __init__(self, file_path: str, num_slots: int, slot_data_size: int):
        if num_slots <= 0 or slot_data_size <= 0:
            raise ValueError("invalid dimensions")
        self.num_slots = num_slots
        self.slot_data_size = slot_data_size
        self.slot_size = 24 + slot_data_size
        self.total_size = num_slots * self.slot_size
        self.write_index = 0
        self.read_index = 0

        self.fd = os.open(file_path, os.O_RDWR | os.O_CREAT, 0o666)
        os.ftruncate(self.fd, self.total_size)
        self.mmap = mmap.mmap(self.fd, self.total_size, access=mmap.ACCESS_WRITE)

    def close(self):
        if hasattr(self, 'mmap') and self.mmap:
            self.mmap.close()
            self.mmap = None
        if hasattr(self, 'fd') and self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def clear(self):
        for i in range(self.num_slots):
            self.set_flag(i, STATE_EMPTY)
            self.set_len(i, 0)
            self.set_seq(i, 0)
            self.set_writer_waiting(i, 0)
            self.set_reader_waiting(i, 0)
        self.write_index = 0
        self.read_index = 0

    def _offset(self, slot: int) -> int:
        return slot * self.slot_size

    def get_flag(self, slot: int) -> int:
        offset = self._offset(slot)
        return struct.unpack_from("<I", self.mmap, offset)[0]

    def set_flag(self, slot: int, val: int):
        offset = self._offset(slot)
        struct.pack_into("<I", self.mmap, offset, val)

    def get_len(self, slot: int) -> int:
        offset = self._offset(slot) + 4
        return struct.unpack_from("<I", self.mmap, offset)[0]

    def set_len(self, slot: int, val: int):
        offset = self._offset(slot) + 4
        struct.pack_into("<I", self.mmap, offset, val)

    def get_seq(self, slot: int) -> int:
        offset = self._offset(slot) + 8
        return struct.unpack_from("<Q", self.mmap, offset)[0]

    def set_seq(self, slot: int, val: int):
        offset = self._offset(slot) + 8
        struct.pack_into("<Q", self.mmap, offset, val)

    def get_writer_waiting(self, slot: int) -> int:
        offset = self._offset(slot) + 16
        return struct.unpack_from("<I", self.mmap, offset)[0]

    def set_writer_waiting(self, slot: int, val: int):
        offset = self._offset(slot) + 16
        struct.pack_into("<I", self.mmap, offset, val)

    def get_reader_waiting(self, slot: int) -> int:
        offset = self._offset(slot) + 20
        return struct.unpack_from("<I", self.mmap, offset)[0]

    def set_reader_waiting(self, slot: int, val: int):
        offset = self._offset(slot) + 20
        struct.pack_into("<I", self.mmap, offset, val)

    def get_payload(self, slot: int, length: int) -> bytes:
        offset = self._offset(slot) + 24
        return self.mmap[offset : offset + length]

    def set_payload(self, slot: int, payload: bytes):
        offset = self._offset(slot) + 24
        self.mmap[offset : offset + len(payload)] = payload

    def write(self, payload: bytes, seq: int, timeout: float, sig_conn: socket.socket):
        if len(payload) > self.slot_data_size:
            raise ValueError(f"payload size {len(payload)} exceeds slot capacity {self.slot_data_size}")

        start = time.time()
        flag_val = self.get_flag(self.write_index)

        while flag_val != STATE_EMPTY:
            self.set_writer_waiting(self.write_index, 1)
            time.sleep(0.0001)  # Force memory barrier / yield for weakly-ordered CPUs
            if self.get_flag(self.write_index) == STATE_EMPTY:
                self.set_writer_waiting(self.write_index, 0)
                break

            poll_timeout = 0.010
            if timeout > 0:
                remaining = timeout - (time.time() - start)
                if remaining <= 0:
                    self.set_writer_waiting(self.write_index, 0)
                    idx = self.write_index
                    self.write_index = (self.write_index + 1) % self.num_slots
                    raise TimeoutError(f"write timeout on slot {idx} waiting for empty signal")
                sig_conn.settimeout(min(remaining, poll_timeout))
            else:
                sig_conn.settimeout(poll_timeout)

            try:
                token = sig_conn.recv(1)
                if not token:
                    self.set_writer_waiting(self.write_index, 0)
                    raise ConnectionAbortedError("connection closed")
            except socket.timeout:
                self.set_writer_waiting(self.write_index, 0)
                flag_val = self.get_flag(self.write_index)
                continue
            except Exception as e:
                self.set_writer_waiting(self.write_index, 0)
                idx = self.write_index
                self.write_index = (self.write_index + 1) % self.num_slots
                raise e

            self.set_writer_waiting(self.write_index, 0)
            flag_val = self.get_flag(self.write_index)

        slot_idx = self.write_index
        self.set_payload(slot_idx, payload)
        self.set_len(slot_idx, len(payload))
        self.set_seq(slot_idx, seq)

        self.set_flag(slot_idx, STATE_WRITTEN)

        if self.get_reader_waiting(slot_idx) == 1:
            self.set_reader_waiting(slot_idx, 0)
            sig_conn.settimeout(1.0)
            try:
                sig_conn.sendall(b'\x01')
            except Exception:
                pass

        self.write_index = (self.write_index + 1) % self.num_slots

    def read(self, timeout: float, sig_conn: socket.socket) -> Tuple[bytes, int]:
        start = time.time()
        flag_val = self.get_flag(self.read_index)

        while flag_val != STATE_WRITTEN:
            self.set_reader_waiting(self.read_index, 1)
            time.sleep(0.0001)  # Force memory barrier / yield for weakly-ordered CPUs
            if self.get_flag(self.read_index) == STATE_WRITTEN:
                self.set_reader_waiting(self.read_index, 0)
                break

            poll_timeout = 0.010
            if timeout > 0:
                remaining = timeout - (time.time() - start)
                if remaining <= 0:
                    self.set_reader_waiting(self.read_index, 0)
                    raise TimeoutError(f"read timeout on slot {self.read_index} waiting for write signal")
                sig_conn.settimeout(min(remaining, poll_timeout))
            else:
                sig_conn.settimeout(poll_timeout)

            try:
                token = sig_conn.recv(1)
                if not token:
                    self.set_reader_waiting(self.read_index, 0)
                    raise ConnectionAbortedError("connection closed")
            except socket.timeout:
                self.set_reader_waiting(self.read_index, 0)
                flag_val = self.get_flag(self.read_index)
                continue
            except Exception as e:
                self.set_reader_waiting(self.read_index, 0)
                raise e

            self.set_reader_waiting(self.read_index, 0)
            flag_val = self.get_flag(self.read_index)

        slot_idx = self.read_index
        length = self.get_len(slot_idx)
        seq = self.get_seq(slot_idx)

        if length > self.slot_data_size:
            raise ValueError(f"invalid payload length {length} on slot {slot_idx}")

        payload = self.get_payload(slot_idx, length)

        self.set_flag(slot_idx, STATE_EMPTY)

        if self.get_writer_waiting(slot_idx) == 1:
            self.set_writer_waiting(slot_idx, 0)
            sig_conn.settimeout(1.0)
            try:
                sig_conn.sendall(b'\x02')
            except Exception:
                pass

        self.read_index = (self.read_index + 1) % self.num_slots
        return payload, seq

    async def async_write(self, payload: bytes, seq: int, timeout: float, sig_reader: asyncio.StreamReader, sig_writer: asyncio.StreamWriter):
        if len(payload) > self.slot_data_size:
            raise ValueError(f"payload size {len(payload)} exceeds slot capacity {self.slot_data_size}")

        start = time.time()
        flag_val = self.get_flag(self.write_index)

        while flag_val != STATE_EMPTY:
            self.set_writer_waiting(self.write_index, 1)
            await asyncio.sleep(0.0001)
            if self.get_flag(self.write_index) == STATE_EMPTY:
                self.set_writer_waiting(self.write_index, 0)
                break

            poll_timeout = 0.010
            actual_timeout = poll_timeout
            if timeout > 0:
                remaining = timeout - (time.time() - start)
                if remaining <= 0:
                    self.set_writer_waiting(self.write_index, 0)
                    idx = self.write_index
                    self.write_index = (self.write_index + 1) % self.num_slots
                    raise TimeoutError(f"write timeout on slot {idx}")
                actual_timeout = min(remaining, poll_timeout)

            try:
                token = await asyncio.wait_for(sig_reader.read(1), timeout=actual_timeout)
                if not token:
                    self.set_writer_waiting(self.write_index, 0)
                    raise ConnectionAbortedError("connection closed")
            except asyncio.TimeoutError:
                self.set_writer_waiting(self.write_index, 0)
                flag_val = self.get_flag(self.write_index)
                continue
            except Exception as e:
                self.set_writer_waiting(self.write_index, 0)
                idx = self.write_index
                self.write_index = (self.write_index + 1) % self.num_slots
                raise e

            self.set_writer_waiting(self.write_index, 0)
            flag_val = self.get_flag(self.write_index)

        slot_idx = self.write_index
        self.set_payload(slot_idx, payload)
        self.set_len(slot_idx, len(payload))
        self.set_seq(slot_idx, seq)

        self.set_flag(slot_idx, STATE_WRITTEN)

        if self.get_reader_waiting(slot_idx) == 1:
            self.set_reader_waiting(slot_idx, 0)
            try:
                sig_writer.write(b'\x01')
                await sig_writer.drain()
            except Exception:
                pass

        self.write_index = (self.write_index + 1) % self.num_slots

    async def async_read(self, timeout: float, sig_reader: asyncio.StreamReader, sig_writer: asyncio.StreamWriter) -> Tuple[bytes, int]:
        start = time.time()
        flag_val = self.get_flag(self.read_index)

        while flag_val != STATE_WRITTEN:
            self.set_reader_waiting(self.read_index, 1)
            await asyncio.sleep(0.0001)
            if self.get_flag(self.read_index) == STATE_WRITTEN:
                self.set_reader_waiting(self.read_index, 0)
                break

            poll_timeout = 0.010
            actual_timeout = poll_timeout
            if timeout > 0:
                remaining = timeout - (time.time() - start)
                if remaining <= 0:
                    self.set_reader_waiting(self.read_index, 0)
                    raise TimeoutError(f"read timeout on slot {self.read_index}")
                actual_timeout = min(remaining, poll_timeout)

            try:
                token = await asyncio.wait_for(sig_reader.read(1), timeout=actual_timeout)
                if not token:
                    self.set_reader_waiting(self.read_index, 0)
                    raise ConnectionAbortedError("connection closed")
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                self.set_reader_waiting(self.read_index, 0)
                raise e

            self.set_reader_waiting(self.read_index, 0)
            flag_val = self.get_flag(self.read_index)

        slot_idx = self.read_index
        length = self.get_len(slot_idx)
        seq = self.get_seq(slot_idx)

        if length > self.slot_data_size:
            raise ValueError(f"invalid payload length {length} on slot {slot_idx}")

        payload = self.get_payload(slot_idx, length)
        self.set_flag(slot_idx, STATE_EMPTY)

        if self.get_writer_waiting(slot_idx) == 1:
            self.set_writer_waiting(slot_idx, 0)
            try:
                sig_writer.write(b'\x02')
                await sig_writer.drain()
            except Exception:
                pass

        self.read_index = (self.read_index + 1) % self.num_slots
        return payload, seq

class Writer:
    def __init__(self, rb: RingBuffer, sig_conn: socket.socket, write_timeout: float):
        self.rb = rb
        self.sig_conn = sig_conn
        self.write_timeout = write_timeout
        self.next_write_seq = 0
        self.write_mu = threading.Lock()

    def write(self, p: bytes) -> int:
        with self.write_mu:
            total = len(p)
            offset = 0
            while offset < total:
                chunk_size = min(total - offset, self.rb.slot_data_size)
                chunk = p[offset : offset + chunk_size]
                self.rb.write(chunk, self.next_write_seq, self.write_timeout, self.sig_conn)
                self.next_write_seq += 1
                offset += chunk_size
            return total

    def close(self):
        if self.sig_conn:
            try:
                self.sig_conn.close()
            except Exception:
                pass
            self.sig_conn = None
        if self.rb:
            self.rb.close()
            self.rb = None

class Reader:
    def __init__(self, rb: RingBuffer, sig_conn: socket.socket, read_timeout: float):
        self.rb = rb
        self.sig_conn = sig_conn
        self.read_timeout = read_timeout
        self.read_buf = bytearray()
        self.read_mu = threading.Lock()

    def read(self, n: int) -> bytes:
        with self.read_mu:
            if n <= 0:
                return b''
            if len(self.read_buf) == 0:
                payload, _ = self.rb.read(self.read_timeout, self.sig_conn)
                self.read_buf.extend(payload)
            
            chunk_size = min(n, len(self.read_buf))
            res = bytes(self.read_buf[:chunk_size])
            del self.read_buf[:chunk_size]
            return res

    def read_full(self, n: int) -> bytes:
        res = bytearray()
        while len(res) < n:
            chunk = self.read(n - len(res))
            if not chunk:
                raise io.BlockingIOError("EOF or connection closed during read_full")
            res.extend(chunk)
        return bytes(res)

    def close(self):
        if self.sig_conn:
            try:
                self.sig_conn.close()
            except Exception:
                pass
            self.sig_conn = None
        if self.rb:
            self.rb.close()
            self.rb = None

def new_connection(
    base_path: str,
    role: str,
    num_slots: int,
    slot_data_size: int,
    write_timeout: float,
    read_timeout: float,
) -> Tuple[Writer, Reader]:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("UNIX Domain Sockets (AF_UNIX) are not supported by Python on this platform (e.g. Windows). Please run inside WSL/Linux.")
    sig_sock_path = base_path + "_sig.sock"
    
    write_path = ""
    read_path = ""

    if role == ROLE_HOST:
        write_path = base_path + "_writer"
        read_path = base_path + "_reader"

        if os.path.exists(sig_sock_path):
            try:
                os.remove(sig_sock_path)
            except Exception:
                pass

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(sig_sock_path)
        listener.listen(2)

        conn1, _ = listener.accept()
        conn2, _ = listener.accept()
        listener.close()

    elif role == ROLE_PLUGIN:
        write_path = base_path + "_reader"
        read_path = base_path + "_writer"

        conn1 = None
        for _ in range(100):
            try:
                c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                c.connect(sig_sock_path)
                conn1 = c
                break
            except Exception:
                time.sleep(0.01)
        if conn1 is None:
            raise TimeoutError("plugin failed to dial first sig connection")

        conn2 = None
        for _ in range(100):
            try:
                c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                c.connect(sig_sock_path)
                conn2 = c
                break
            except Exception:
                time.sleep(0.01)
        if conn2 is None:
            conn1.close()
            raise TimeoutError("plugin failed to dial second sig connection")

    else:
        raise ValueError(f"invalid role: {role}, must be 'Host' or 'Plugin'")

    write_rb = RingBuffer(write_path, num_slots, slot_data_size)
    write_rb.clear()

    read_rb = RingBuffer(read_path, num_slots, slot_data_size)

    if role == ROLE_HOST:
        writer = Writer(write_rb, conn1, write_timeout)
        reader = Reader(read_rb, conn2, read_timeout)
    else:
        writer = Writer(write_rb, conn2, write_timeout)
        reader = Reader(read_rb, conn1, read_timeout)

    return writer, reader

class AsyncWriter:
    def __init__(self, rb: RingBuffer, sig_reader: asyncio.StreamReader, sig_writer: asyncio.StreamWriter, write_timeout: float, close_event: asyncio.Event = None):
        self.rb = rb
        self.sig_reader = sig_reader
        self.sig_writer = sig_writer
        self.write_timeout = write_timeout
        self.next_write_seq = 0
        self.write_mu = asyncio.Lock()
        self.close_event = close_event

    async def write(self, p: bytes) -> int:
        async with self.write_mu:
            total = len(p)
            offset = 0
            while offset < total:
                chunk_size = min(total - offset, self.rb.slot_data_size)
                chunk = p[offset : offset + chunk_size]
                await self.rb.async_write(chunk, self.next_write_seq, self.write_timeout, self.sig_reader, self.sig_writer)
                self.next_write_seq += 1
                offset += chunk_size
            return total

    async def close(self):
        if self.close_event:
            self.close_event.set()
        if self.sig_writer:
            try:
                self.sig_writer.close()
                await self.sig_writer.wait_closed()
            except Exception:
                pass
            self.sig_writer = None
        self.sig_reader = None
        if self.rb:
            self.rb.close()
            self.rb = None

class AsyncReader:
    def __init__(self, rb: RingBuffer, sig_reader: asyncio.StreamReader, sig_writer: asyncio.StreamWriter, read_timeout: float, close_event: asyncio.Event = None):
        self.rb = rb
        self.sig_reader = sig_reader
        self.sig_writer = sig_writer
        self.read_timeout = read_timeout
        self.read_buf = bytearray()
        self.read_mu = asyncio.Lock()
        self.close_event = close_event

    async def read(self, n: int) -> bytes:
        async with self.read_mu:
            if n <= 0:
                return b''
            if not self.read_buf:
                payload, _ = await self.rb.async_read(self.read_timeout, self.sig_reader, self.sig_writer)
                self.read_buf.extend(payload)
            
            chunk_size = min(n, len(self.read_buf))
            res = bytes(self.read_buf[:chunk_size])
            del self.read_buf[:chunk_size]
            return res

    async def read_exactly(self, n: int) -> bytes:
        res = bytearray()
        while len(res) < n:
            chunk = await self.read(n - len(res))
            if not chunk:
                raise io.BlockingIOError("EOF or connection closed during read_exactly")
            res.extend(chunk)
        return bytes(res)

    async def close(self):
        if self.close_event:
            self.close_event.set()
        if self.sig_writer:
            try:
                self.sig_writer.close()
                await self.sig_writer.wait_closed()
            except Exception:
                pass
            self.sig_writer = None
        self.sig_reader = None
        if self.rb:
            self.rb.close()
            self.rb = None

async def async_new_connection(
    base_path: str,
    role: str,
    num_slots: int,
    slot_data_size: int,
    write_timeout: float,
    read_timeout: float,
) -> Tuple[AsyncWriter, AsyncReader]:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("UNIX Domain Sockets (AF_UNIX) are not supported on this platform.")
    sig_sock_path = base_path + "_sig.sock"
    
    write_path = ""
    read_path = ""

    if role == ROLE_HOST:
        write_path = base_path + "_writer"
        read_path = base_path + "_reader"

        if os.path.exists(sig_sock_path):
            try:
                os.remove(sig_sock_path)
            except Exception:
                pass

        connections = []
        loop = asyncio.get_running_loop()
        server_future = loop.create_future()
        ev1 = asyncio.Event()
        ev2 = asyncio.Event()

        async def client_connected_cb(reader, writer):
            connections.append((reader, writer))
            if len(connections) == 2:
                server_future.set_result(connections)
                await ev2.wait()
            else:
                await ev1.wait()

        server = await asyncio.start_unix_server(client_connected_cb, path=sig_sock_path)
        try:
            conns = await server_future
        finally:
            server.close()
            loop.create_task(server.wait_closed())

        (conn1_reader, conn1_writer), (conn2_reader, conn2_writer) = conns

    elif role == ROLE_PLUGIN:
        write_path = base_path + "_reader"
        read_path = base_path + "_writer"
        ev1 = None
        ev2 = None

        conn1_reader, conn1_writer = None, None
        for _ in range(100):
            try:
                conn1_reader, conn1_writer = await asyncio.open_unix_connection(sig_sock_path)
                break
            except Exception:
                await asyncio.sleep(0.01)
        if conn1_reader is None:
            raise TimeoutError("plugin failed to dial first sig connection")

        conn2_reader, conn2_writer = None, None
        for _ in range(100):
            try:
                conn2_reader, conn2_writer = await asyncio.open_unix_connection(sig_sock_path)
                break
            except Exception:
                await asyncio.sleep(0.01)
        if conn2_reader is None:
            conn1_writer.close()
            await conn1_writer.wait_closed()
            raise TimeoutError("plugin failed to dial second sig connection")

    else:
        raise ValueError(f"invalid role: {role}, must be 'Host' or 'Plugin'")

    write_rb = RingBuffer(write_path, num_slots, slot_data_size)
    write_rb.clear()

    read_rb = RingBuffer(read_path, num_slots, slot_data_size)

    if role == ROLE_HOST:
        writer = AsyncWriter(write_rb, conn1_reader, conn1_writer, write_timeout, ev1)
        reader = AsyncReader(read_rb, conn2_reader, conn2_writer, read_timeout, ev2)
    else:
        writer = AsyncWriter(write_rb, conn2_reader, conn2_writer, write_timeout, ev2)
        reader = AsyncReader(read_rb, conn1_reader, conn1_writer, read_timeout, ev1)

    return writer, reader

