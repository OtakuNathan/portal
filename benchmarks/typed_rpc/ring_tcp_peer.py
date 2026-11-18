"""Accept one rpc_ring_tcp_benchmark run, check each typed frame, then acknowledge EOF."""
import argparse
import socket
import struct


def mix(h, value):
    for byte in value.to_bytes(8, "little"):
        h = (h ^ byte) * 1099511628211 & ((1 << 64) - 1)
    return h


def receive(conn):
    seed = 14695981039346656037
    blob = mix(mix(seed, 20), 65536)
    fingerprint = mix(mix(mix(seed, mix(seed, 11)), 1), blob)
    header = struct.pack("<4sHHQIIQI", b"PTR1", 1, 0, 1, 1, 65503, fingerprint, 65499)
    frames = (header + b"a" * 65499, header + b"b" * 65499)
    buf = bytearray(len(frames[0]))
    view = memoryview(buf)
    for i in range(512):
        offset = 0
        while offset < len(buf):
            taken = conn.recv_into(view[offset:])
            if not taken:
                raise EOFError(f"incomplete frame {i}")
            offset += taken
        if buf != frames[i % 2]:
            raise ValueError(f"frame {i} differs from the expected wire bytes")
    if conn.recv(1):
        raise ValueError("unexpected trailing bytes")
    conn.sendall(b"K")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bind_address")
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    with socket.socket() as listener:
        listener.bind((args.bind_address, args.port))
        listener.listen(1)
        print(listener.getsockname()[1], flush=True)
        conn, _ = listener.accept()
        with conn:
            conn.settimeout(10)
            receive(conn)
