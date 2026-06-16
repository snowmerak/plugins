import argparse
import sys
from ringbuf_py import new_connection, ROLE_HOST, ROLE_PLUGIN

def main():
    parser = argparse.ArgumentParser(description="Python Ringbuffer Demo")
    parser.add_argument("--role", type=str, default="Host", choices=["Host", "Plugin"], help="Role: Host or Plugin")
    parser.add_argument("--path", type=str, default="shm_comm_cross", help="Base path for shm/sockets")
    args = parser.parse_args()

    role = args.role
    base_path = args.path

    print(f"Starting Python Ringbuffer Demo as {role}...")

    try:
        writer, reader = new_connection(
            base_path=base_path,
            role=role,
            num_slots=4,
            slot_data_size=1024,
            write_timeout=15.0,
            read_timeout=15.0,
        )
    except Exception as e:
        print(f"failed to create connection: {e}", file=sys.stderr)
        sys.exit(1)

    try:
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
