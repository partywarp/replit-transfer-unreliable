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

# These values control the simulated unreliable network.
DATA_DROP_RATE = 0.15
ACK_DROP_RATE = 0.15
MAX_ACK_DELAY = 0.35

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
    chunks: dict[int, str] = field(default_factory=dict)
    duplicate_packets: int = 0
    packet_attempts: int = 0


@app.get("/")
async def health() -> dict[str, object]:
    return {
        "version": 2.0,
        "secret_count": 4,
        "python_version": sys.version,
        "data_drop_rate": DATA_DROP_RATE,
        "ack_drop_rate": ACK_DROP_RATE,
    }


@app.get("/6767420", response_class=HTMLResponse)
async def sixseven() -> str:
    return "<h1> sixseven </h1>"


async def send_help(ws: WebSocket) -> None:
    await ws.send_text(
        "Accepted commands:\n"
        "CONNECT <ip> <port>\n"
        "CLOSE\n"
        "ROB <amount>\n"
        "START <filename> <total_chunks> <sha256>\n"
        "DATA <sequence_number> <text>\n"
        "STATUS\n"
        "DONE"
    )


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    connection: str | None = None
    transfer: Transfer | None = None

    try:
        while True:
            message = await ws.receive_text()

            if not message.strip():
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

                transfer = None
                connection = None
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
                    await ws.send_text(
                        f"Stole ${amount:.2f} from {connection}"
                    )
                    await ws.send_text("The police caught you!")
                    await ws.send_text("You were removed.")
                    connection = None
                else:
                    await ws.send_text(
                        "You cannot rob your current connection!"
                    )

            elif command == "START":
                parts = message.split()

                if len(parts) != 4:
                    await ws.send_text(
                        "Usage: START <filename> <total_chunks> <sha256>"
                    )
                    continue

                if connection != NY_TIMES_ADDRESS:
                    await ws.send_text(
                        "File transfers are only accepted by NY Times."
                    )
                    continue

                if transfer is not None:
                    await ws.send_text(
                        "A transfer is already active. Use CLOSE to cancel it."
                    )
                    continue

                filename = parts[1]

                try:
                    total_chunks = int(parts[2])
                except ValueError:
                    await ws.send_text(
                        "The total chunk count must be an integer."
                    )
                    continue

                if total_chunks < 1 or total_chunks > 10_000:
                    await ws.send_text(
                        "The total chunk count must be between 1 and 10000."
                    )
                    continue

                expected_sha256 = parts[3].lower()

                if (
                    len(expected_sha256) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in expected_sha256
                    )
                ):
                    await ws.send_text(
                        "The SHA-256 value must contain 64 hexadecimal characters."
                    )
                    continue

                transfer = Transfer(
                    filename=filename,
                    total_chunks=total_chunks,
                    expected_sha256=expected_sha256,
                )

                await ws.send_text(
                    f"READY {filename} {total_chunks}"
                )

            elif command == "DATA":
                if connection != NY_TIMES_ADDRESS:
                    await ws.send_text(
                        "You must connect to NY Times first."
                    )
                    continue

                if transfer is None:
                    await ws.send_text(
                        "No transfer active. Use START first."
                    )
                    continue

                # maxsplit=2 preserves spaces and newlines inside the payload.
                parts = message.split(" ", maxsplit=2)

                if len(parts) != 3:
                    await ws.send_text(
                        "Usage: DATA <sequence_number> <text>"
                    )
                    continue

                try:
                    sequence = int(parts[1])
                except ValueError:
                    await ws.send_text(
                        "The sequence number must be an integer."
                    )
                    continue

                if sequence < 0 or sequence >= transfer.total_chunks:
                    await ws.send_text(
                        f"Sequence must be between 0 and "
                        f"{transfer.total_chunks - 1}."
                    )
                    continue

                payload = parts[2]
                transfer.packet_attempts += 1

                # Simulate a packet disappearing before reaching the receiver.
                # No reply is sent, so the client must time out and retransmit.
                if random.random() < DATA_DROP_RATE:
                    continue

                if sequence in transfer.chunks:
                    transfer.duplicate_packets += 1
                else:
                    transfer.chunks[sequence] = payload

                await asyncio.sleep(
                    random.uniform(0.0, MAX_ACK_DELAY)
                )

                # Simulate an acknowledgement being lost.
                # The chunk was stored, but the client never receives the ACK.
                if random.random() < ACK_DROP_RATE:
                    continue

                await ws.send_text(f"ACK {sequence}")

            elif command == "STATUS":
                if transfer is None:
                    await ws.send_text("No transfer active.")
                    continue

                received = len(transfer.chunks)

                missing = [
                    str(sequence)
                    for sequence in range(transfer.total_chunks)
                    if sequence not in transfer.chunks
                ]

                if missing:
                    missing_text = ",".join(missing[:25])

                    if len(missing) > 25:
                        missing_text += ",..."

                    await ws.send_text(
                        f"RECEIVED {received}/{transfer.total_chunks} "
                        f"MISSING {missing_text}"
                    )
                else:
                    await ws.send_text(
                        f"RECEIVED {received}/{transfer.total_chunks} "
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
                    missing_text = ",".join(
                        str(sequence)
                        for sequence in missing[:25]
                    )

                    if len(missing) > 25:
                        missing_text += ",..."

                    await ws.send_text(
                        f"TRANSFER INCOMPLETE MISSING {missing_text}"
                    )
                    continue

                reconstructed = "".join(
                    transfer.chunks[sequence]
                    for sequence in range(transfer.total_chunks)
                )

                actual_sha256 = hashlib.sha256(
                    reconstructed.encode("utf-8")
                ).hexdigest()

                if actual_sha256 != transfer.expected_sha256:
                    await ws.send_text(
                        f"CHECKSUM FAILED {actual_sha256}"
                    )
                    continue

                await ws.send_text(
                    f"TRANSFER COMPLETE "
                    f"{transfer.filename} "
                    f"{len(reconstructed.encode('utf-8'))} bytes "
                    f"{transfer.packet_attempts} attempts "
                    f"{transfer.duplicate_packets} duplicates"
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
