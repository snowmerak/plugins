import argparse
import sys
import time
import asyncio
from ringbuf_py import new_connection, async_new_connection, ROLE_HOST, ROLE_PLUGIN

async def async_main(args, num_slots, slot_size):
    role = args.role
    base_path = args.path
    count = args.count
    size = args.size

    try:
        writer, reader = await async_new_connection(
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
                    await writer.write(payload)
                    await reader.read_exactly(size)
                elapsed = time.time() - start
                ops = count / elapsed
                latency = (elapsed / count) * 1000000.0 # in microseconds
                print(f"BENCHMARK_RESULT: {count} rounds, total time: {elapsed:.4f}s, {ops:.2f} ops/sec, avg latency: {latency:.2f} us")
            else:
                for i in range(count):
                    await reader.read_exactly(size)
                    await writer.write(payload)
        else:
            if role == ROLE_HOST:
                msg = b"hello from python host"
                print(f"Host writing: '{msg.decode()}'")
                await writer.write(msg)

                buf = await reader.read(100)
                print(f"Host read: '{buf.decode()}'")
            else:
                buf = await reader.read(100)
                print(f"Plugin read: '{buf.decode()}'")

                msg = b"hello from python plugin"
                print(f"Plugin writing: '{msg.decode()}'")
                await writer.write(msg)

        print("Python Demo finished successfully.")
    except Exception as e:
        print(f"execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        await writer.close()
        await reader.close()

def main():
    parser = argparse.ArgumentParser(description="Python Ringbuffer Demo")
    parser.add_argument("--role", type=str, default="Host", choices=["Host", "Plugin"], help="Role: Host or Plugin")
    parser.add_argument("--path", type=str, default="shm_comm_cross", help="Base path for shm/sockets")
    parser.add_argument("--count", type=int, default=1, help="Number of iterations (benchmark if > 1)")
    parser.add_argument("--size", type=int, default=1024, help="Message size for benchmark")
    parser.add_argument("--async", dest="async_mode", action="store_true", help="Run in asyncio mode")
    args = parser.parse_args()

    role = args.role
    base_path = args.path
    count = args.count
    size = args.size
    async_mode = args.async_mode

    num_slots = 8 if count > 1 else 4
    slot_size = 4096 if count > 1 else 1024

    if count > 1:
        print(f"Starting Python Ringbuffer Demo as {role} (Benchmark: count={count}, size={size}, async={async_mode})...")
    else:
        print(f"Starting Python Ringbuffer Demo as {role} (async={async_mode})...")

    if async_mode:
        asyncio.run(async_main(args, num_slots, slot_size))
    else:
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
