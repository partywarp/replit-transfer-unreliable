# pyright: strict

import asyncio
import hashlib
import os
import random
import sys
from dataclasses import dataclass, field
from collections import deque
from typing import Final

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
    "10.0.0.8:23": "white house",
    "10.0.0.9:24": "i knew it",
}

COMPUTER_IP: Final[str] = "10.0.2.100"
LOCAL_ROUTER: Final[str] = "10.0.1.1"
ROUTER_COUNT: Final[int] = 6

# Leave NETWORK_SEED unset for a new topology after each restart.
# Set it in Render when you need repeatable behavior.
random_source = random.Random(os.environ.get("NETWORK_SEED"))


def get_host_ips() -> list[str]:
    return sorted({address.rsplit(":", maxsplit=1)[0] for address in online_hosts})


def first_router_hop(
    start: str,
    destination: str,
    adjacency: dict[str, list[str]],
) -> str | None:
    if start == destination:
        return destination

    queue: deque[tuple[str, str]] = deque()
    visited: set[str] = {start}

    for neighbor in adjacency[start]:
        queue.append((neighbor, neighbor))
        visited.add(neighbor)

    while queue:
        current, first_hop = queue.popleft()

        if current == destination:
            return first_hop

        for neighbor in adjacency[current]:
            if neighbor in visited:
                continue

            visited.add(neighbor)
            queue.append((neighbor, first_hop))

    return None


def build_network(
    host_ips: list[str],
    router_count: int,
) -> tuple[
    dict[str, list[str]],
    dict[str, str],
    dict[str, dict[str, str]],
]:
    routers = [f"10.0.1.{number}" for number in range(1, router_count + 1)]

    adjacency: dict[str, list[str]] = {router: [] for router in routers}

    # Each new router attaches to one existing router.
    # This creates a connected tree with no routing loops.
    for index in range(1, len(routers)):
        router = routers[index]
        parent = random_source.choice(routers[:index])

        adjacency[parent].append(router)
        adjacency[router].append(parent)

    # Avoid attaching hosts directly to the student's local router
    # so traceroute normally contains multiple hops.
    possible_host_routers = routers[1:] if len(routers) > 1 else routers

    host_attachment: dict[str, str] = {}

    for host_ip in host_ips:
        host_attachment[host_ip] = random_source.choice(possible_host_routers)

    routing_tables: dict[str, dict[str, str]] = {router: {} for router in routers}

    for router in routers:
        for host_ip, attached_router in host_attachment.items():
            if router == attached_router:
                # The host is directly connected to this router.
                routing_tables[router][host_ip] = host_ip
                continue

            next_hop = first_router_hop(
                router,
                attached_router,
                adjacency,
            )

            if next_hop is None:
                raise RuntimeError(f"No route from {router} to {attached_router}")

            routing_tables[router][host_ip] = next_hop

    return adjacency, host_attachment, routing_tables


def build_reverse_routes(
    router_count: int,
) -> tuple[
    dict[str, list[str]],
    dict[str, str],
]:
    routers = [f"10.0.1.{number}" for number in range(1, router_count + 1)]

    adjacency: dict[str, list[str]] = {router: [] for router in routers}

    # Generate a second independent router tree.
    for index in range(1, len(routers)):
        router = routers[index]
        parent = random_source.choice(routers[:index])

        adjacency[parent].append(router)
        adjacency[router].append(parent)

    next_hops_to_computer: dict[str, str] = {}

    for router in routers:
        if router == LOCAL_ROUTER:
            continue

        next_hop = first_router_hop(
            router,
            LOCAL_ROUTER,
            adjacency,
        )

        if next_hop is None:
            raise RuntimeError(f"No reverse route from {router}")

        next_hops_to_computer[router] = next_hop

    return adjacency, next_hops_to_computer


router_links, host_attachment, routing_tables = build_network(
    get_host_ips(),
    ROUTER_COUNT,
)
reverse_router_links, reverse_next_hops = build_reverse_routes(ROUTER_COUNT)
computer_routing_table = {
    "default": LOCAL_ROUTER,
}


def trace_packet(
    destination_ip: str,
    ttl: int,
) -> str:
    if ttl < 1:
        return "TTL must be at least 1."

    if destination_ip not in host_attachment:
        return f"DESTINATION UNREACHABLE {destination_ip}"

    # The virtual computer sends everything to its default gateway.
    current_router = computer_routing_table["default"]
    remaining_ttl = ttl
    visited: set[str] = set()

    while True:
        if current_router in visited:
            return f"ROUTING LOOP AT {current_router}"

        visited.add(current_router)

        # Every router decreases the TTL before forwarding.
        remaining_ttl -= 1

        if remaining_ttl == 0:
            return f"TTL EXPIRED AT {current_router}"

        next_hop = routing_tables[current_router].get(destination_ip)

        if next_hop is None:
            return f"NO ROUTE FROM {current_router}"

        if next_hop == destination_ip:
            return f"REACHED {destination_ip}"

        current_router = next_hop


def trace_from_host_to_computer(
    source_ip: str,
    ttl: int,
) -> str:
    if ttl < 1:
        return "TTL must be at least 1."

    attached_router = host_attachment.get(source_ip)

    if attached_router is None:
        return f"UNKNOWN HOST {source_ip}"

    current_router = attached_router
    remaining_ttl = ttl
    visited: set[str] = set()

    while True:
        if current_router in visited:
            return f"ROUTING LOOP AT {current_router}"

        visited.add(current_router)

        # The current router processes and decreases the TTL.
        remaining_ttl -= 1

        if remaining_ttl == 0:
            return f"TTL EXPIRED AT {current_router}"

        # The local router forwards directly to the computer.
        if current_router == LOCAL_ROUTER:
            return f"REACHED {COMPUTER_IP}"

        next_hop = reverse_next_hops.get(current_router)

        if next_hop is None:
            return f"NO ROUTE FROM {current_router}"

        current_router = next_hop


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
        "version": 4.0,
        "secret_count": 4,
        "python_version": sys.version,
        "virtual_frame_bytes": VIRTUAL_FRAME_BYTES,
        "frame_loss_rate": FRAME_LOSS_RATE,
        "router_count": ROUTER_COUNT,
        "own_ip": COMPUTER_IP
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
        "TRACE <ip> <ttl>\n"
        "REMOTE_TRACE <ttl> \n"
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
            _, *args = message.split()

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
            elif command == "TRACE":
                if len(args) != 2:
                    await ws.send_text("Usage: TRACE <destination-ip> <ttl>")
                    continue

                destination_ip = args[0]

                try:
                    ttl = int(args[1])
                except ValueError:
                    await ws.send_text("TTL must be an integer.")
                    continue

                await ws.send_text(trace_packet(destination_ip, ttl))
            elif command == "REMOTE_TRACE":
                if connection is None:
                    await ws.send_text("You must connect to a host first.")
                    continue

                if len(args) != 1:
                    await ws.send_text("Usage: REMOTE_TRACE <ttl>")
                    continue

                try:
                    ttl = int(args[0])
                except ValueError:
                    await ws.send_text("TTL must be an integer.")
                    continue

                source_ip = connection.rsplit(":", maxsplit=1)[0]

                await ws.send_text(
                    trace_from_host_to_computer(
                        source_ip,
                        ttl,
                    )
                )
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
