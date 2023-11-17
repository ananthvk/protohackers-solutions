import struct
import asyncio
import traceback
from datetime import datetime

encoding = "utf-8"

CLIENT_UNIDENTIFIED, CLIENT_CAMERA, CLIENT_DISPATCHER = range(3)
HEARTBEAT_NOT_SET, HEARTBEAT_SET = range(2)


def info(writer, *args, **kwargs):
    addr = writer.get_extra_info("peername")
    addr = f"{addr[0]}:{addr[1]}"
    print(f"{datetime.now().strftime('%T.%f')} [{addr}]", *args, **kwargs)


def log(writer, *args, **kwargs):
    addr = writer.get_extra_info("peername")
    addr = f"{addr[0]}:{addr[1]}"
    print(f"{datetime.now().strftime('%T.%f')} [{addr}]", *args, **kwargs)


def log2(*args, **kwargs):
    print(f"{datetime.now().strftime('%T.%f')} [INFO]", *args, **kwargs)


class Client:
    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.identity = CLIENT_UNIDENTIFIED
        self.hearbeat = HEARTBEAT_NOT_SET
        self.tasks = set()
        self.closed = False

    async def close(self):
        self.closed = True
        try:
            log(self.writer, "Awaiting closure")
            self.writer.close()
            await self.writer.wait_closed()
            log(self.writer, "Connection closed")
            # TODO: Delete all tasks
        except:
            pass


# Sends an error message to the client and also disconnects the client
class ErrorHandler:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.writer = client.writer

    async def handle(self, message: bytes, close_connection=True):
        try:
            length = len(message)
            if length > 255:
                raise SystemExit("Logic error: Server sent message of length > 255")
            self.writer.write(struct.pack(f"!BB{length}s", 0x10, length, message))
            await self.writer.drain()
        finally:
            if close_connection:
                try:
                    self.writer.close()
                    await self.writer.wait_closed()
                except:
                    # TODO Close all other tasks related to this client
                    pass


class PlateHandler:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.reader = client.reader

    # Attempts to read a plate packet from the stream
    async def handle(self):
        plate_length = await self.reader.read(1)
        plate_length = plate_length[0]
        plate = await self.reader.readexactly(plate_length + 4)
        plate, timestamp = struct.unpack(f"!{plate_length}sI", plate)
        return plate, timestamp


class TicketWriter:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.writer = client.writer

    async def handle(
        self,
        plate: bytes,
        road: int,
        mile1: int,
        timestamp1: int,
        mile2: int,
        timestamp2: int,
        speed: int,
    ) -> None:
        plate_length = len(plate)
        self.writer.write(
            struct.pack(
                f"!BB{plate_length}sHHIHIH",
                0x21,
                plate_length,
                plate,
                road,
                mile1,
                timestamp1,
                mile2,
                timestamp2,
                speed * 100,
            )
        )
        await self.writer.drain()


class HeartBeatHandler:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.reader = client.reader

    """
    Returns the hearbeat as integer in milliseconds
    The client sends it in decisecond, one decisecond = 1/10 seconds
    """

    async def handle(self) -> int:
        interval = await self.reader.readexactly(4)
        return struct.unpack("!I", interval)[0] * 100


class HeartBeatWriter:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.writer = client.writer

    async def handle(self):
        self.writer.write(bytearray([0x41]))
        await self.writer.drain()


class CameraHandler:
    def __init__(self, client: Client) -> None:
        self.reader = client.reader
        self.client = client
        self.readers = {
            # OP code   # Handler
            0x20: PlateHandler,
            0x40: HeartBeatHandler,
        }

    async def handle(self):
        log(self.client.writer, "The client has identified itself as a camera")
        data = await self.reader.readexactly(6)
        road, mile, limit = struct.unpack("!HHH", data)
        log(self.client.writer, f"Camera {road: {road}, mile: {mile}, limit: {limit}}")
        while True:
            # The client has identified itself as a camera
            # The client can send only WantHeartbeat and Plate messages
            pass


class DispatcherHandler:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.reader = client.reader

    async def handle(self):
        num_roads = await self.reader.readexactly(1)
        num_roads = num_roads[0]
        roads = await self.reader.read(2 * num_roads)
        return struct.unpack(f"!{num_roads}H", roads)


class SpeedDaemonServer:
    def __init__(self, loop) -> None:
        self.loop = loop
        self.handlers = {
            # OP code   # Handler
            0x80: CameraHandler,
            0x81: DispatcherHandler,
        }

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        client = Client(reader, writer)
        try:
            opcode = await reader.read(1)
            if opcode:
                opcode = opcode[0]
                instance = self.handlers[opcode](client)
                while True:
                    await instance.handle()
        except Exception as e:
            log(writer, f"ERROR: {traceback.format_exc()}")
            try:
                await ErrorHandler(client).handle(
                    traceback.format_exc()[:254].encode("utf-8"), close_connection=False
                )
            except Exception as e:
                log(writer, "ERROR: error writing error message")
        finally:
            await client.close()


async def main():
    loop = asyncio.get_running_loop()
    sds = SpeedDaemonServer(loop)
    server = await asyncio.start_server(
        sds.handle_connection, host="0.0.0.0", port=8000
    )
    log2("Started server on 0.0.0.0:8000")
    async with server:
        await server.serve_forever()


asyncio.run(main())
