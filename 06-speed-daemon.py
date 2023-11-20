import struct
import asyncio
import traceback
from datetime import datetime

encoding = "utf-8"


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
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.tasks = set()
        self.closed = False
        self.loop = loop

    async def close(self):
        if not self.closed:
            self.closed = True
            addr = self.writer.get_extra_info("peername")
            try:
                log(self.writer, "Awaiting closure")
                self.writer.close()
                await self.writer.wait_closed()
                log(self.writer, "Connection closed")
            except:
                pass

            for task in self.tasks:
                try:
                    task.cancel()
                    log2(addr, "Cancelled a task")
                except:
                    pass


# Sends an error message to the client and also disconnect the client
class ErrorWriter:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.writer = client.writer

    async def write(self, message: bytes, close_connection=True):
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
                    await self.client.close()


class PlateReader:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.reader = client.reader

    # Attempts to read a plate packet from the stream
    async def read(self):
        plate_length = await self.reader.read(1)
        plate_length = plate_length[0]
        plate = await self.reader.readexactly(plate_length + 4)
        plate, timestamp = struct.unpack(f"!{plate_length}sI", plate)
        return plate, timestamp


class TicketWriter:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.writer = client.writer

    async def write(
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


class HeartBeatReader:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.reader = client.reader

    async def read(self) -> int:
        interval = await self.reader.readexactly(4)
        return struct.unpack("!I", interval)[0]


class HeartBeatTask:
    def __init__(self, client: Client, interval: int) -> None:  # In deciseconds
        self.client = client
        self.writer = client.writer
        self.task = None
        self.interval = interval

    async def write(self):
        # Exceptions inside a client can stop the main event loop from working
        # So handle exceptions here
        try:
            while True:
                log(self.client.writer, "Sending hearbeat to client")
                self.writer.write(bytearray([0x41]))
                await self.writer.drain()
                await asyncio.sleep(self.interval / 10)
        except:
            log(
                self.client.writer,
                "Error while sending hearbeat ... closing connection",
            )
        finally:
            await self.client.close()

    async def start(self):
        log(
            self.client.writer,
            f"Got a request to send hearbeat at interval: {self.interval/10}s",
        )
        if self.interval == 0:
            return
        log(self.client.writer, "Created hearbeat task")
        self.task = self.client.loop.create_task(self.write())
        self.client.tasks.add(self.task)
        self.task.add_done_callback(self.client.tasks.discard)


class CameraHandler:
    def __init__(self, client: Client) -> None:
        self.reader = client.reader
        self.client = client
        self.readers = {
            # OP code   # Reader
            0x20: PlateReader,
            0x40: HeartBeatReader,
        }

    async def handle(self):
        log(self.client.writer, "The client has identified itself as a camera")
        data = await self.reader.readexactly(6)
        road, mile, limit = struct.unpack("!HHH", data)
        log(
            self.client.writer, f"Camera {{road: {road}, mile: {mile}, limit: {limit}}}"
        )
        while True:
            # The client has identified itself as a camera
            # The client can send only WantHeartbeat and Plate messages
            opcode = await self.reader.read(1)
            if opcode:
                opcode = opcode[0]
                instance = self.readers[opcode](self.client)
                # We have gotten a plate
                parsed = await instance.read()
                if opcode == 0x20:
                    pass
                if opcode == 0x40:
                    # Remove the heart beat reader from self.readers
                    # So that any more heart beat requests from this client will be an error
                    del self.readers[0x40]
                    await HeartBeatTask(self.client, parsed).start()
            else:
                # The client disconnected
                log(self.client.writer, "Disconnected")
                return


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
        client = Client(reader, writer, self.loop)
        try:
            opcode = await reader.read(1)
            if opcode:
                opcode = opcode[0]
                instance = self.handlers[opcode](client)
                await instance.handle()
        except Exception as e:
            log(writer, f"ERROR: {traceback.format_exc()}")
            try:
                await ErrorWriter(client).write(
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
