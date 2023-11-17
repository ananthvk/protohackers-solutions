import multiprocessing

# multiprocessing.set_start_method("spawn", True)

import asyncio
import traceback
import struct
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys

TIMEOUT_DURATION = 30
ADDR = "0.0.0.0"
PORT = 8000
MAX_WORKERS = 30
# Works with thread pool executor
# But for some reason, mean is 0 when ProcessPoolExecutor is used, find out why
pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
store = dict()


def parse_data(data):
    # https://docs.python.org/3/library/struct.html
    message_type, int1, int2 = struct.unpack("!cii", data)
    if not (message_type == b"Q" or message_type == b"I"):
        raise ValueError("Invalid message type")
    return message_type, int1, int2


def compute_mean(session: str, mintime: int, maxtime: int):
    try:
        if mintime > maxtime:
            return 0
        sum_of_prices = 0
        count_of_prices = 0
        for val in store.get(session, set()):
            if val[0] >= mintime and val[0] <= maxtime:
                sum_of_prices += val[1]
                count_of_prices += 1

        if count_of_prices == 0:
            return 0
        return sum_of_prices // count_of_prices
    except:
        return -1


# Unoptimized query as it scans all the records to find the average
async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    # Due to some reason when using get_event_loop(), mean becomes 0
    event_loop = asyncio.get_running_loop()
    addr = writer.get_extra_info("peername")
    addr = f"{addr[0]}:{addr[1]}"
    print(f"{datetime.now().strftime('%T.%f')} [{addr}] connected")
    while True:
        try:
            # :( Not there in Python 3.9
            # async with asyncio.timeout(TIMEOUT_DURATION):
            data = await asyncio.wait_for(
                reader.readexactly(9), timeout=TIMEOUT_DURATION
            )
            mtype, int1, int2 = parse_data(data)
            if mtype == b"I":
                price = (int1, int2)
                # print(f"{datetime.now().strftime('%T.%f')} [{addr}] INSERT {price}")
                if store.get(addr) is None:
                    store[addr] = {price}
                else:
                    store[addr].add(price)
            if mtype == b"Q":
                task = event_loop.run_in_executor(pool, compute_mean, addr, int1, int2)
                print(
                    f"{datetime.now().strftime('%T.%f')} [{addr}] SUBMITTED QUERY {int1} to {int2}"
                )
                mean = await task
                if mean == -1:
                    raise ValueError("Could not compute mean")
                print(
                    f"{datetime.now().strftime('%T.%f')} [{addr}] QUERY {int1} to {int2} : {mean}"
                )
                packed = struct.pack("!i", mean)
                writer.write(packed)
                await writer.drain()

        except asyncio.TimeoutError:
            print(
                f"{datetime.now().strftime('%T.%f')} [{addr}] timed out",
                file=sys.stderr,
            )
            break
        except asyncio.IncompleteReadError:
            print(
                f"{datetime.now().strftime('%T.%f')} [{addr}] sent incomplete request",
                file=sys.stderr,
            )
            break
        except (ValueError, struct.error):
            print(
                f"{datetime.now().strftime('%T.%f')} [{addr}] error while processing request",
                file=sys.stderr,
            )
            break
        except Exception as e:
            print(
                f"{datetime.now().strftime('%T.%f')} [{addr}] error while processing request {traceback.format_exc()}",
                file=sys.stderr,
            )
            break

    writer.close()
    # Remove the client's data
    try:
        del store[addr]
    except:
        pass
    await writer.wait_closed()


async def main():
    event_loop = asyncio.get_running_loop()
    server = await asyncio.start_server(handle_request, ADDR, PORT)
    print(f"{datetime.now().strftime('%T.%f')} Started server on {ADDR}:{PORT}")
    async with server:
        await server.serve_forever()


asyncio.run(main())
