use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use memmap2::MmapMut;

const STATE_EMPTY: u32 = 0;
const STATE_WRITTEN: u32 = 1;

pub const ROLE_HOST: &str = "Host";
pub const ROLE_PLUGIN: &str = "Plugin";

#[cfg(unix)]
pub use std::os::unix::net::{UnixListener, UnixStream};

#[cfg(windows)]
pub use uds_windows::{UnixListener, UnixStream};

pub struct RingBuffer {
	mmap: MmapMut,
	num_slots: usize,
	slot_size: usize,
	slot_data_size: usize,
	write_index: usize,
	read_index: usize,
}

impl RingBuffer {
	pub fn new(file_path: &str, num_slots: usize, slot_data_size: usize) -> io::Result<Self> {
		if num_slots == 0 || slot_data_size == 0 {
			return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid dimensions"));
		}
		let slot_size = 24 + slot_data_size;
		let total_size = num_slots * slot_size;

		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(file_path)?;
		file.set_len(total_size as u64)?;

		let mmap = unsafe { MmapMut::map_mut(&file)? };

		Ok(Self {
			mmap,
			num_slots,
			slot_size,
			slot_data_size,
			write_index: 0,
			read_index: 0,
		})
	}

	pub fn clear(&mut self) {
		for i in 0..self.num_slots {
			self.get_flag_atomic(i).store(STATE_EMPTY, Ordering::SeqCst);
			self.get_len_atomic(i).store(0, Ordering::SeqCst);
			self.get_seq_atomic(i).store(0, Ordering::SeqCst);
			self.get_writer_waiting_atomic(i).store(0, Ordering::SeqCst);
			self.get_reader_waiting_atomic(i).store(0, Ordering::SeqCst);
		}
		self.write_index = 0;
		self.read_index = 0;
	}

	pub fn write(
		&mut self,
		payload: &[u8],
		seq: u64,
		timeout: Duration,
		sig_conn: &mut UnixStream,
	) -> io::Result<()> {
		if payload.len() > self.slot_data_size {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("payload size {} exceeds slot capacity {}", payload.len(), self.slot_data_size),
			));
		}

		let flag_ptr = self.get_flag_atomic(self.write_index);
		let writer_waiting_ptr = self.get_writer_waiting_atomic(self.write_index);
		let start = Instant::now();

		while flag_ptr.load(Ordering::SeqCst) != STATE_EMPTY {
			writer_waiting_ptr.store(1, Ordering::SeqCst);

			if flag_ptr.load(Ordering::SeqCst) == STATE_EMPTY {
				writer_waiting_ptr.store(0, Ordering::SeqCst);
				break;
			}

			if timeout > Duration::ZERO {
				let elapsed = start.elapsed();
				if elapsed >= timeout {
					writer_waiting_ptr.store(0, Ordering::SeqCst);
					let idx = self.write_index;
					self.write_index = (self.write_index + 1) % self.num_slots;
					return Err(io::Error::new(
						io::ErrorKind::TimedOut,
						format!("write timeout on slot {} waiting for empty signal", idx),
					));
				}
				sig_conn.set_read_timeout(Some(timeout - elapsed))?;
			} else {
				sig_conn.set_read_timeout(None)?;
			}

			let mut buf = [0u8; 1];
			match sig_conn.read(&mut buf) {
				Ok(0) => {
					writer_waiting_ptr.store(0, Ordering::SeqCst);
					return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "connection closed"));
				}
				Ok(_) => {}
				Err(e) => {
					writer_waiting_ptr.store(0, Ordering::SeqCst);
					self.write_index = (self.write_index + 1) % self.num_slots;
					return Err(e);
				}
			}
			writer_waiting_ptr.store(0, Ordering::SeqCst);
		}

		let slot_idx = self.write_index;
		let dest = self.get_payload_slice_mut(slot_idx);
		dest[..payload.len()].copy_from_slice(payload);

		self.get_len_atomic(slot_idx).store(payload.len() as u32, Ordering::SeqCst);
		self.get_seq_atomic(slot_idx).store(seq, Ordering::SeqCst);

		flag_ptr.store(STATE_WRITTEN, Ordering::SeqCst);

		let reader_waiting_ptr = self.get_reader_waiting_atomic(slot_idx);
		if reader_waiting_ptr.compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
			sig_conn.set_write_timeout(Some(Duration::from_secs(1)))?;
			let token = [0x01u8];
			let _ = sig_conn.write(&token);
		}

		self.write_index = (self.write_index + 1) % self.num_slots;
		Ok(())
	}

	pub fn read(
		&mut self,
		timeout: Duration,
		sig_conn: &mut UnixStream,
	) -> io::Result<(Vec<u8>, u64)> {
		let flag_ptr = self.get_flag_atomic(self.read_index);
		let reader_waiting_ptr = self.get_reader_waiting_atomic(self.read_index);
		let start = Instant::now();

		while flag_ptr.load(Ordering::SeqCst) != STATE_WRITTEN {
			reader_waiting_ptr.store(1, Ordering::SeqCst);

			if flag_ptr.load(Ordering::SeqCst) == STATE_WRITTEN {
				reader_waiting_ptr.store(0, Ordering::SeqCst);
				break;
			}

			if timeout > Duration::ZERO {
				let elapsed = start.elapsed();
				if elapsed >= timeout {
					reader_waiting_ptr.store(0, Ordering::SeqCst);
					return Err(io::Error::new(
						io::ErrorKind::TimedOut,
						format!("read timeout on slot {} waiting for write signal", self.read_index),
					));
				}
				sig_conn.set_read_timeout(Some(timeout - elapsed))?;
			} else {
				sig_conn.set_read_timeout(None)?;
			}

			let mut buf = [0u8; 1];
			match sig_conn.read(&mut buf) {
				Ok(0) => {
					reader_waiting_ptr.store(0, Ordering::SeqCst);
					return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "connection closed"));
				}
				Ok(_) => {}
				Err(e) => {
					reader_waiting_ptr.store(0, Ordering::SeqCst);
					return Err(e);
				}
			}
			reader_waiting_ptr.store(0, Ordering::SeqCst);
		}

		let length = self.get_len_atomic(self.read_index).load(Ordering::SeqCst) as usize;
		let seq = self.get_seq_atomic(self.read_index).load(Ordering::SeqCst);

		if length > self.slot_data_size {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("invalid payload length {} on slot {}", length, self.read_index),
			));
		}

		let payload = self.get_payload_slice(self.read_index)[..length].to_vec();

		flag_ptr.store(STATE_EMPTY, Ordering::SeqCst);

		let writer_waiting_ptr = self.get_writer_waiting_atomic(self.read_index);
		if writer_waiting_ptr.compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
			sig_conn.set_write_timeout(Some(Duration::from_secs(1)))?;
			let token = [0x02u8];
			let _ = sig_conn.write(&token);
		}

		self.read_index = (self.read_index + 1) % self.num_slots;
		Ok((payload, seq))
	}

	fn get_flag_atomic(&self, slot: usize) -> &'static AtomicU32 {
		let offset = slot * self.slot_size;
		unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU32) }
	}

	fn get_len_atomic(&self, slot: usize) -> &'static AtomicU32 {
		let offset = slot * self.slot_size + 4;
		unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU32) }
	}

	fn get_seq_atomic(&self, slot: usize) -> &'static AtomicU64 {
		let offset = slot * self.slot_size + 8;
		unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU64) }
	}

	fn get_writer_waiting_atomic(&self, slot: usize) -> &'static AtomicU32 {
		let offset = slot * self.slot_size + 16;
		unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU32) }
	}

	fn get_reader_waiting_atomic(&self, slot: usize) -> &'static AtomicU32 {
		let offset = slot * self.slot_size + 20;
		unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU32) }
	}

	fn get_payload_slice_mut(&mut self, slot: usize) -> &mut [u8] {
		let offset = slot * self.slot_size + 24;
		&mut self.mmap[offset..offset + self.slot_data_size]
	}

	fn get_payload_slice(&self, slot: usize) -> &[u8] {
		let offset = slot * self.slot_size + 24;
		&self.mmap[offset..offset + self.slot_data_size]
	}
}

pub struct Writer {
	rb: RingBuffer,
	sig_conn: UnixStream,
	write_timeout: Duration,
	next_write_seq: u64,
	write_mu: Mutex<()>,
}

impl Writer {
	pub fn new(rb: RingBuffer, sig_conn: UnixStream, write_timeout: Duration) -> Self {
		Self {
			rb,
			sig_conn,
			write_timeout,
			next_write_seq: 0,
			write_mu: Mutex::new(()),
		}
	}
}

impl Write for Writer {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		let _guard = self.write_mu.lock().unwrap();

		let total = buf.len();
		let mut p = buf;

		while !p.is_empty() {
			let mut chunk_size = p.len();
			if chunk_size > self.rb.slot_data_size {
				chunk_size = self.rb.slot_data_size;
			}

			self.rb.write(&p[..chunk_size], self.next_write_seq, self.write_timeout, &mut self.sig_conn)?;
			self.next_write_seq += 1;
			p = &p[chunk_size..];
		}

		Ok(total)
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

pub struct Reader {
	rb: RingBuffer,
	sig_conn: UnixStream,
	read_timeout: Duration,
	read_buf: Vec<u8>,
	read_mu: Mutex<()>,
}

impl Reader {
	pub fn new(rb: RingBuffer, sig_conn: UnixStream, read_timeout: Duration) -> Self {
		Self {
			rb,
			sig_conn,
			read_timeout,
			read_buf: Vec::new(),
			read_mu: Mutex::new(()),
		}
	}
}

impl Read for Reader {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let _guard = self.read_mu.lock().unwrap();

		if buf.is_empty() {
			return Ok(0);
		}

		if self.read_buf.is_empty() {
			let (payload, _) = self.rb.read(self.read_timeout, &mut self.sig_conn)?;
			self.read_buf = payload;
		}

		let n = std::cmp::min(buf.len(), self.read_buf.len());
		buf[..n].copy_from_slice(&self.read_buf[..n]);
		self.read_buf.drain(..n);
		Ok(n)
	}
}

pub fn new_connection(
	base_path: &str,
	role: &str,
	num_slots: usize,
	slot_data_size: usize,
	write_timeout: Duration,
	read_timeout: Duration,
) -> io::Result<(Writer, Reader)> {
	let sig_sock_path = format!("{}_sig.sock", base_path);

	let write_path: String;
	let read_path: String;

	let (conn1, conn2) = if role == ROLE_HOST {
		write_path = format!("{}_writer", base_path);
		read_path = format!("{}_reader", base_path);

		let _ = std::fs::remove_file(&sig_sock_path);
		let listener = UnixListener::bind(&sig_sock_path)?;

		let (conn1, _) = listener.accept()?;
		let (conn2, _) = listener.accept()?;

		(conn1, conn2)
	} else if role == ROLE_PLUGIN {
		write_path = format!("{}_reader", base_path);
		read_path = format!("{}_writer", base_path);

		let mut conn1 = None;
		let mut conn2 = None;

		for _ in 0..100 {
			if let Ok(c) = UnixStream::connect(&sig_sock_path) {
				conn1 = Some(c);
				break;
			}
			std::thread::sleep(Duration::from_millis(10));
		}
		let conn1 = conn1.ok_or_else(|| {
			io::Error::new(io::ErrorKind::TimedOut, "plugin failed to dial first sig connection")
		})?;

		for _ in 0..100 {
			if let Ok(c) = UnixStream::connect(&sig_sock_path) {
				conn2 = Some(c);
				break;
			}
			std::thread::sleep(Duration::from_millis(10));
		}
		let conn2 = conn2.ok_or_else(|| {
			io::Error::new(io::ErrorKind::TimedOut, "plugin failed to dial second sig connection")
		})?;

		(conn1, conn2)
	} else {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("invalid role: {}, must be 'Host' or 'Plugin'", role),
		));
	};

	let mut write_rb = RingBuffer::new(&write_path, num_slots, slot_data_size)?;
	write_rb.clear();

	let read_rb = RingBuffer::new(&read_path, num_slots, slot_data_size)?;

	let (writer, reader) = if role == ROLE_HOST {
		(
			Writer::new(write_rb, conn1, write_timeout),
			Reader::new(read_rb, conn2, read_timeout),
		)
	} else {
		(
			Writer::new(write_rb, conn2, write_timeout),
			Reader::new(read_rb, conn1, read_timeout),
		)
	};

	Ok((writer, reader))
}
