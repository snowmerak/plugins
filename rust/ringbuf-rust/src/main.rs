use std::env;
use std::io::{Read, Write};
use std::time::Duration;
use ringbuf_rust::new_connection;

fn main() -> std::io::Result<()> {
	let args: Vec<String> = env::args().collect();
	let mut role = "Host";
	let mut base_path = "shm_comm_cross";

	let mut i = 1;
	while i < args.len() {
		if args[i] == "--role" && i + 1 < args.len() {
			role = &args[i + 1];
			i += 2;
		} else if args[i] == "--path" && i + 1 < args.len() {
			base_path = &args[i + 1];
			i += 2;
		} else {
			i += 1;
		}
	}

	println!("Starting Rust Ringbuffer Demo as {}...", role);

	let (mut writer, mut reader) = new_connection(
		base_path,
		role,
		4,
		1024,
		Duration::from_secs(15),
		Duration::from_secs(15),
	)?;

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

	println!("Rust Demo finished successfully.");
	Ok(())
}
