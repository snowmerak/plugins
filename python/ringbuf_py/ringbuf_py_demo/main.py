import argparse
import sys
import time
from ringbuf_py import new_connection, ROLE_HOST, ROLE_PLUGIN

def main():
    parser = argparse.ArgumentParser(description="Python Ringbuffer Demo")
    parser.add_argument("--role", type=str, default="Host", choices=["Host", "Plugin"], help="Role: Host or Plugin")
    parser.add_argument("--path", type=str, default="shm_comm_cross", help="Base path for shm/sockets")
    parser.add_argument("--count", type=int, default=1, help="Number of iterations (benchmark if > 1)")
    parser.add_argument("--size", type=int, default=1024, help="Message size for benchmark")
    args = parser.parse_args()

    role = args.role
    base_path = args.path
    count = args.count
    size = args.size

    num_slots = 8 if count > 1 else 4
    slot_size = 4096 if count > 1 else 1024

    if count > 1:
        print(f"Starting Python Ringbuffer Demo as {role} (Benchmark: count={count}, size={size})...")
    else:
        print(f"Starting Python Ringbuffer Demo as {role}...")

    try:
        writer, reader = new_connection(
            base_path=base_path,
            role=role,
            num_slots=num_slots,
            slot_data_size=slot_size,
            write_timeout=15.0,
            read_timeout=15.0,
        )
    except Exception as e:
        print(f"failed to create connection: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        if count > 1:
            payload = b'a' * size
            if role == ROLE_HOST:
                start = time.time()
                for i in range(count):
                    writer.write(payload)
                    reader.read_full(size)
                elapsed = time.time() - start
                ops = count / elapsed
                latency = (elapsed / count) * 1000000.0 # in microseconds
                print(f"BENCHMARK_RESULT: {count} rounds, total time: {elapsed:.4f}s, {ops:.2f} ops/sec, avg latency: {latency:.2f} us")
            else:
                for i in range(count):
                    reader.read_full(size)
                    writer.write(payload)
        else:
            if role == ROLE_HOST:
                msg = b"hello from python host"
                print(f"Host writing: '{msg.decode()}'")
                writer.write(msg)

                buf = reader.read(100)
                print(f"Host read: '{buf.decode()}'")
            else:
                buf = reader.read(100)
                print(f"Plugin read: '{buf.decode()}'")

                msg = b"hello from python plugin"
                print(f"Plugin writing: '{msg.decode()}'")
                writer.write(msg)

        print("Python Demo finished successfully.")
    except Exception as e:
        print(f"execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        writer.close()
        reader.close()

if __name__ == "__main__":
    main()
