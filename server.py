# pyright: strict

import asyncio
import hashlib
import os
import random
import sys
from dataclasses import dataclass, field

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

NY_TIMES_ADDRESS = "10.0.0.7:21"

# The simulated network divides each DATA payload into 100-byte frames.
# Every frame independently has a 20% chance of being lost.
VIRTUAL_FRAME_BYTES = 100
FRAME_LOSS_RATE = 0.20

# These settings simulate transmission time and per-message overhead.
BASE_DELAY_SECONDS = 0.02
LINK_BYTES_PER_SECOND = 5_000.0
JITTER_SECONDS = 0.01

DATA_HEADER_BYTES = 20
ACK_BYTES = 8


online_hosts: dict[str, str] = {
    "10.0.0.1:20": "The Bank",
    "10.0.0.7:21": "NY Times",
    "10.0.0.7:23": "white house",
    "10.0.0.7:24": "i knew it",
}


@dataclass
class Transfer:
    filename: str
    total_chunks: int
    expected_sha256: str
    chunks: dict[int, str] = field(default_factory=dict[int, str])

    packet_attempts: int = 0
    dropped_attempts: int = 0
    duplicate_packets: int = 0
    simulated_wire_bytes: int = 0


@app.get("/")
async def health() -> dict[str, object]:
    return {
        "version": 3.0,
        "secret_count": 4,
        "python_version": sys.version,
        "virtual_frame_bytes": VIRTUAL_FRAME_BYTES,
        "frame_loss_rate": FRAME_LOSS_RATE,
    }


@app.get("/6767420", response_class=HTMLResponse)
async def sixseven() -> str:
    return "<h1> sixseven </h1>"


async def send_help(ws: WebSocket) -> None:
    for line in (
        "Accepted commands:\n"
        "CONNECT <ip> <port>\n"
        "CLOSE\n"
        "ROB <amount>\n"
        "START <filename> <total_chunks> <sha256>\n"
        "DATA <sequence_number> <text>\n"
        "STATUS\n"
        "DONE\n"
        "HELP".splitlines()
    ):
        await ws.send_text(line)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    connection: str | None = None
    transfer: Transfer | None = None

    try:
        while True:
            message = await ws.receive_text()

            if not message:
                await ws.send_text("Empty command.")
                continue

            command = message.split(maxsplit=1)[0].upper()

            if command == "CONNECT":
                parts = message.split()

                if len(parts) != 3:
                    await ws.send_text("Usage: CONNECT <ip> <port>")
                    continue

                if connection is not None:
                    await ws.send_text("Please close your active connection.")
                    continue

                key = f"{parts[1]}:{parts[2]}"

                if key not in online_hosts:
                    await ws.send_text("Host offline (or incorrect port).")
                    continue

                connection = key
                await ws.send_text(f"CONNECTED: {online_hosts[key]}")

            elif command == "CLOSE":
                if connection is None:
                    await ws.send_text("You have no connection.")
                    continue

                connection = None
                transfer = None

                await ws.send_text("Connection closed.")

            elif command == "ROB":
                parts = message.split()

                if len(parts) != 2:
                    await ws.send_text("Usage: ROB <amount>")
                    continue

                if connection is None:
                    await ws.send_text("You have no connection.")
                    continue

                try:
                    amount = float(parts[1])
                except ValueError:
                    await ws.send_text("Amount must be a number.")
                    continue

                if online_hosts[connection] == "The Bank":
                    await ws.send_text(f"Stole ${amount:.2f} from {connection}")
                    await ws.send_text("The police caught you!")
                    await ws.send_text("You were removed.")

                    connection = None
                    transfer = None
                else:
                    await ws.send_text("You cannot rob your current connection!")

            elif command == "START":
                parts = message.split()

                if len(parts) != 4:
                    await ws.send_text(
                        "Usage: START " "<filename> <total_chunks> <sha256>"
                    )
                    continue

                if connection != NY_TIMES_ADDRESS:
                    await ws.send_text("File transfers are only accepted by NY Times.")
                    continue

                if transfer is not None:
                    await ws.send_text(
                        "A transfer is already active. " "Use CLOSE to cancel it."
                    )
                    continue

                filename = parts[1]

                try:
                    total_chunks = int(parts[2])
                except ValueError:
                    await ws.send_text("The total chunk count must be an integer.")
                    continue

                if total_chunks < 1 or total_chunks > 10_000:
                    await ws.send_text(
                        "The total chunk count must be " "between 1 and 10000."
                    )
                    continue

                expected_sha256 = parts[3].lower()

                valid_sha256 = len(expected_sha256) == 64 and all(
                    character in "0123456789abcdef" for character in expected_sha256
                )

                if not valid_sha256:
                    await ws.send_text(
                        "The SHA-256 value must contain " "64 hexadecimal characters."
                    )
                    continue

                transfer = Transfer(
                    filename=filename,
                    total_chunks=total_chunks,
                    expected_sha256=expected_sha256,
                )

                await ws.send_text(f"READY {filename} {total_chunks}")

            elif command == "DATA":
                if connection != NY_TIMES_ADDRESS:
                    await ws.send_text("You must connect to NY Times first.")
                    continue

                if transfer is None:
                    await ws.send_text("No transfer active. Use START first.")
                    continue

                # partition() preserves spaces and newlines in the payload.
                _, separator, remainder = message.partition(" ")

                if not separator:
                    await ws.send_text("Usage: DATA <sequence_number> <text>")
                    continue

                sequence_text, separator, payload = remainder.partition(" ")

                if not separator:
                    await ws.send_text("Usage: DATA <sequence_number> <text>")
                    continue

                try:
                    sequence = int(sequence_text)
                except ValueError:
                    await ws.send_text("The sequence number must be an integer.")
                    continue

                if sequence < 0 or sequence >= transfer.total_chunks:
                    await ws.send_text(
                        f"Sequence must be between 0 and "
                        f"{transfer.total_chunks - 1}."
                    )
                    continue

                payload_bytes = payload.encode("utf-8")

                frame_count = max(
                    1,
                    (len(payload_bytes) + VIRTUAL_FRAME_BYTES - 1)
                    // VIRTUAL_FRAME_BYTES,
                )

                transfer.packet_attempts += 1
                transfer.simulated_wire_bytes += len(payload_bytes) + DATA_HEADER_BYTES

                transmission_delay = (
                    BASE_DELAY_SECONDS
                    + len(payload_bytes) / LINK_BYTES_PER_SECOND
                    + random.uniform(
                        0.0,
                        JITTER_SECONDS,
                    )
                )

                await asyncio.sleep(transmission_delay)

                # The entire DATA chunk fails if any frame is lost.
                frame_was_lost = any(
                    random.random() < FRAME_LOSS_RATE for _ in range(frame_count)
                )

                if frame_was_lost:
                    transfer.dropped_attempts += 1

                    # No response is sent. The client must time out
                    # and retransmit the complete chunk.
                    continue

                if sequence in transfer.chunks:
                    transfer.duplicate_packets += 1
                else:
                    transfer.chunks[sequence] = payload

                transfer.simulated_wire_bytes += ACK_BYTES

                await ws.send_text(f"ACK {sequence}")

            elif command == "STATUS":
                if transfer is None:
                    await ws.send_text("No transfer active.")
                    continue

                missing = [
                    sequence
                    for sequence in range(transfer.total_chunks)
                    if sequence not in transfer.chunks
                ]

                if missing:
                    missing_text = ",".join(str(sequence) for sequence in missing[:25])

                    if len(missing) > 25:
                        missing_text += ",..."

                    await ws.send_text(
                        f"RECEIVED "
                        f"{len(transfer.chunks)}/"
                        f"{transfer.total_chunks} "
                        f"MISSING {missing_text}"
                    )
                else:
                    await ws.send_text(
                        f"RECEIVED "
                        f"{len(transfer.chunks)}/"
                        f"{transfer.total_chunks} "
                        "MISSING none"
                    )

            elif command == "DONE":
                if transfer is None:
                    await ws.send_text("No transfer active.")
                    continue

                missing = [
                    sequence
                    for sequence in range(transfer.total_chunks)
                    if sequence not in transfer.chunks
                ]

                if missing:
                    missing_text = ",".join(str(sequence) for sequence in missing[:25])

                    if len(missing) > 25:
                        missing_text += ",..."

                    await ws.send_text(
                        f"TRANSFER INCOMPLETE " f"MISSING {missing_text}"
                    )
                    continue

                reconstructed = "".join(
                    transfer.chunks[sequence]
                    for sequence in range(transfer.total_chunks)
                )

                reconstructed_bytes = reconstructed.encode("utf-8")

                actual_sha256 = hashlib.sha256(reconstructed_bytes).hexdigest()

                if actual_sha256 != transfer.expected_sha256:
                    await ws.send_text(f"CHECKSUM FAILED {actual_sha256}")
                    continue

                await ws.send_text(
                    f"TRANSFER COMPLETE "
                    f"{transfer.filename} "
                    f"{len(reconstructed_bytes)} bytes "
                    f"{transfer.packet_attempts} attempts "
                    f"{transfer.dropped_attempts} dropped "
                    f"{transfer.duplicate_packets} duplicates "
                    f"{transfer.simulated_wire_bytes} "
                    f"simulated-wire-bytes"
                )

                transfer = None

            elif command == "HELP":
                await send_help(ws)

            else:
                await send_help(ws)

    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "9000"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )
