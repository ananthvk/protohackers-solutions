import os
import math
from collections import deque 

os.environ["PYTHONASYNCIODEBUG"] = "1"
import logging
import struct
import asyncio
import traceback
from datetime import datetime

# Global data store

# Contains a mapping road->dispatchers
dispatchers = dict()
# Contains mapping plate: mile, timestamp
plates = dict()
# Ticket queue: road -> ticket (as bytes)
tickets = dict()
# last ticket sent
last_ticket = dict()
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
            log(self.writer, "Sending error message: ", message.decode('utf-8'))
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
        log(self.writer, f"Writing ticket: {plate_length} {plate} {road} {mile1} {timestamp1} {mile2} {timestamp2} {int(speed*100)}")
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
                int(speed * 100),
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
                if self.client.closed:
                    break
                log(self.client.writer, "Sending hearbeat to client")
                self.writer.write(bytearray([0x41]))
                await self.writer.drain()
                await asyncio.sleep(self.interval / 10)
        except Exception as e:
            log(
                self.client.writer,
                "Error while sending hearbeat ... closing connection",
                traceback.format_exc(),
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
    def __init__(self, client: Client, heartbeat_set) -> None:
        self.reader = client.reader
        self.client = client
        self.readers = {
            # OP code   # Reader
            0x20: PlateReader,
        }
        if not heartbeat_set:
            self.readers = {
                # OP code   # Reader
                0x20: PlateReader,
                0x40: HeartBeatReader,
            }
        else:
            # Hearbeat has already been set, should not accept anymore hearbeat responses
            self.readers = {
                # OP code   # Reader
                0x20: PlateReader,
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
            if not opcode;
                # The client disconnected
                log(self.client.writer, "Disconnected")
                return
            opcode = opcode[0]
            instance = self.readers[opcode](self.client)
            # We have gotten a plate
            parsed = await instance.read()
            if opcode == 0x40:
                # Remove the heart beat reader from self.readers
                # So that any more heart beat requests from this client will be an error
                del self.readers[0x40]
                await HeartBeatTask(self.client, parsed).start()
            if opcode == 0x20:
                plate = parsed[0]
                timestamp = parsed[1]
                log(self.client.writer, f"Got Plate {{{plate} at {timestamp}}}")
                if plates.get(plate) is None:
                    # This is the first plate we have received
                    plates[plate] = [(mile, timestamp)]
                else:
                    plates[plate].append((mile, timestamp))
                    plates[plate].sort()
                    log(self.client.writer, f"There is information about the client, checking if a ticket is needed")
                    for i in range(1, len(plates[plate])):
                        # Check adjacent observations
                        mile2 = plates[plate][i][0] 
                        mile1 = plates[plate][i-1][0]
                        timestamp2 = plates[plate][i][1] 
                        timestamp1 = plates[plate][i-1][1]
                        
                        if timestamp1 > timestamp2:
                            # Swap everything
                            mile1, mile2 = mile2, mile1
                            timestamp2, timestamp1 = timestamp1, timestamp2

                        distance =  mile2 - mile1 
                        t = timestamp2 - timestamp1 
                        v = abs((distance/t)*60*60) # Convert miles/s to miles/h
                        if v - limit >= 0.5:
                            # The client was overspeeding
                            log(self.client.writer, f"{plate} was overspeeding with speed {v} on road {road}")
                            road_dispatchers = dispatchers.get(road)

                            if road_dispatchers is None:
                                # There are no active dispatchers for this road, wait for a one to connect
                                log(self.client.writer, f"There are no active dispatchers for {road}, waiting to send ticket to {plate}")
                                if tickets.get(road) is None:
                                    tickets[road] = deque()
                                tickets[road].append((plate, road, mile1, timestamp1, mile2, timestamp2, v))
                            else:
                                today = math.floor(timestamp / 86400)
                                if last_ticket.get(plate, -1) == today:
                                    # Do not send the ticket as we have already dispatched it
                                    log(self.client.writer, f"A ticket was already sent to {plate} on {road} for this day {today}, last: {last_ticket.get(plate, -1)}")
                                    pass
                                else:
                                    not_sent = True
                                    for dispatcher in road_dispatchers:
                                        if not dispatcher.closed:
                                            # Send the ticket
                                            last_ticket[plate] = today
                                            log(self.client.writer, f"There was an active dispatcher, sending ticket to {plate}, {road}")
                                            await TicketWriter(dispatcher).write(plate, road, mile1, timestamp1, mile2, timestamp2, v)
                                            not_sent = False
                                            break
                                    if not_sent:
                                        # There are no active dispatchers for this road, wait for a one to connect
                                        log(self.client.writer, f"There are no open dispatchers for {road}, waiting to send ticket to {plate}")
                                        if tickets.get(road) is None:
                                            tickets[road] = deque()
                                        tickets[road].append((plate, road, mile1, timestamp1, mile2, timestamp2, v))


class DispatcherHandler:
    def __init__(self, client: Client, hearbeat_set) -> None:
        self.client = client
        self.reader = client.reader
        self.readers = {0x40: HeartBeatReader}
        self.tasks = set()
        if not hearbeat_set:
            self.readers = {0x40: HeartBeatReader}
        else:
            self.readers = dict()

    async def handle(self):
        log(self.client.writer, "The client has identifed itself as a dispatcher")
        num_roads = await self.reader.readexactly(1)
        num_roads = num_roads[0]
        roads = await self.reader.read(2 * num_roads)
        roads = struct.unpack(f"!{num_roads}H", roads)
        for road in roads:
            if not dispatchers.get(road):
                dispatchers[road] = [self.client]
            else:
                dispatchers[road].append(self.client)
            if tickets.get(road) is not None:
                log(self.client.writer, "A pending ticket for this road was found, sending it")
                # task = asyncio.create_task(TicketWriter(self.client).write(*tickets[road].pop()))
                # self.tasks.add(task)
                # task.add_done_callback(self.tasks.discard)
                try:
                    while len(tickets[road]) != 0:
                        ticket = tickets[road].popleft()
                        await TicketWriter(self.client).write(*ticket)
                except:
                    log(self.client.writer, "ERROR, couldn't send ticket", traceback.format_exc())

        log(self.client.writer, f"Dispatcher {{roads: {roads}}}")
        while True:
            # The client has identified itself as a dispatcher
            # The only valid client request is WantHeartbeat
            opcode = await self.reader.read(1)
            if opcode:
                opcode = opcode[0]
                instance = self.readers[opcode](self.client)
                parsed = await instance.read()
                if opcode == 0x40:
                    # Remove the heart beat reader from self.readers
                    # So that any more heart beat requests from this client will be an error
                    del self.readers[0x40]
                    await HeartBeatTask(self.client, parsed).start()
            else:
                # The client disconnected
                log(self.client.writer, "Disconnected")
                return


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
            heartbeat_set = False
            if opcode:
                opcode = opcode[0]
                # Check if it is a hearbeat request
                if opcode == 0x40:
                    heartbeat_set = True
                    parsed = await HeartBeatReader(client).read()
                    await HeartBeatTask(client, parsed).start()
                    opcode = (await reader.read(1))[0]

                instance = self.handlers[opcode](client, heartbeat_set)
                await instance.handle()
        except Exception as e:
            log(writer, f"ERROR: {traceback.format_exc()}")
            try:
                await ErrorWriter(client).write(
                    traceback.format_exc().encode("utf-8"), close_connection=False
                )
            except Exception as e:
                log(writer, "ERROR: error writing error message")
        finally:
            await client.close()


async def main():
    loop = asyncio.get_running_loop()
    # Remove these in production
    logging.basicConfig(level=logging.DEBUG)
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    # ======
    sds = SpeedDaemonServer(loop)
    server = await asyncio.start_server(
        sds.handle_connection, host="0.0.0.0", port=8000
    )
    log2("Started server on 0.0.0.0:8000")
    async with server:
        await server.serve_forever()


asyncio.run(main(), debug=True)
# asyncio.run(main())
