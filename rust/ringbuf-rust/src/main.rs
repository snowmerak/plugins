use std::env;
use std::io::{Read, Write};
use std::time::{Duration, Instant};
use ringbuf_rust::{new_connection, new_connection_async};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
	let args: Vec<String> = env::args().collect();
	let mut role = "Host";
	let mut base_path = "shm_comm_cross";
	let mut count = 1;
	let mut size = 1024;
	let mut is_async = false;

	let mut i = 1;
	while i < args.len() {
		if args[i] == "--role" && i + 1 < args.len() {
			role = &args[i + 1];
			i += 2;
		} else if args[i] == "--path" && i + 1 < args.len() {
			base_path = &args[i + 1];
			i += 2;
		} else if args[i] == "--count" && i + 1 < args.len() {
			count = args[i + 1].parse().unwrap_or(1);
			i += 2;
		} else if args[i] == "--size" && i + 1 < args.len() {
			size = args[i + 1].parse().unwrap_or(1024);
			i += 2;
		} else if args[i] == "--async" {
			is_async = true;
			i += 1;
		} else {
			i += 1;
		}
	}

	let num_slots = if count > 1 { 8 } else { 4 };
	let slot_size = if count > 1 { 4096 } else { 1024 };

	if count > 1 {
		println!(
			"Starting Rust Ringbuffer Demo as {} (Benchmark: count={}, size={}, async={})...",
			role, count, size, is_async
		);
	} else {
		println!("Starting Rust Ringbuffer Demo as {} (async={})...", role, is_async);
	}

	if is_async {
		let (mut writer, mut reader) = new_connection_async(
			base_path,
			role,
			num_slots,
			slot_size,
			Duration::from_secs(15),
			Duration::from_secs(15),
		).await?;

		if count > 1 {
			let payload = vec![0u8; size];
			let mut read_buf = vec![0u8; size];

			let start = Instant::now();
			if role == "Host" {
				for _ in 0..count {
					writer.write(&payload).await?;
					reader.read_exact(&mut read_buf).await?;
				}
				let elapsed = start.elapsed();
				let ops = count as f64 / elapsed.as_secs_f64();
				let latency = elapsed.as_micros() as f64 / count as f64;
				println!(
					"BENCHMARK_RESULT: {} rounds, total time: {:?}, {:.2} ops/sec, avg latency: {:.2} us",
					count, elapsed, ops, latency
				);
			} else {
				for _ in 0..count {
					reader.read_exact(&mut read_buf).await?;
					writer.write(&payload).await?;
				}
			}
		} else {
			if role == "Host" {
				let msg = b"hello from rust host";
				println!("Host writing: '{}'", String::from_utf8_lossy(msg));
				writer.write(msg).await?;

				let mut buf = vec![0u8; 100];
				let n = reader.read(&mut buf).await?;
				println!("Host read: '{}'", String::from_utf8_lossy(&buf[..n]));
			} else {
				let mut buf = vec![0u8; 100];
				let n = reader.read(&mut buf).await?;
				println!("Plugin read: '{}'", String::from_utf8_lossy(&buf[..n]));

				let msg = b"hello from rust plugin";
				println!("Plugin writing: '{}'", String::from_utf8_lossy(msg));
				writer.write(msg).await?;
			}
		}
	} else {
		let (mut writer, mut reader) = new_connection(
			base_path,
			role,
			num_slots,
			slot_size,
			Duration::from_secs(15),
			Duration::from_secs(15),
		)?;

		if count > 1 {
			let payload = vec![0u8; size];
			let mut read_buf = vec![0u8; size];

			let start = Instant::now();
			if role == "Host" {
				for _ in 0..count {
					writer.write_all(&payload)?;
					writer.flush()?;
					reader.read_exact(&mut read_buf)?;
				}
				let elapsed = start.elapsed();
				let ops = count as f64 / elapsed.as_secs_f64();
				let latency = elapsed.as_micros() as f64 / count as f64;
				println!(
					"BENCHMARK_RESULT: {} rounds, total time: {:?}, {:.2} ops/sec, avg latency: {:.2} us",
					count, elapsed, ops, latency
				);
			} else {
				for _ in 0..count {
					reader.read_exact(&mut read_buf)?;
					writer.write_all(&payload)?;
					writer.flush()?;
				}
			}
		} else {
			if role == "Host" {
				let msg = b"hello from rust host";
				println!("Host writing: '{}'", String::from_utf8_lossy(msg));
				writer.write_all(msg)?;
				writer.flush()?;

				let mut buf = vec![0u8; 100];
				let n = reader.read(&mut buf)?;
				println!("Host read: '{}'", String::from_utf8_lossy(&buf[..n]));
			} else {
				let mut buf = vec![0u8; 100];
				let n = reader.read(&mut buf)?;
				println!("Plugin read: '{}'", String::from_utf8_lossy(&buf[..n]));

				let msg = b"hello from rust plugin";
				println!("Plugin writing: '{}'", String::from_utf8_lossy(msg));
				writer.write_all(msg)?;
				writer.flush()?;
			}
		}
	}

	println!("Rust Demo finished successfully.");
	Ok(())
}
