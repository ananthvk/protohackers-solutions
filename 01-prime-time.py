import asyncio
import sympy
import json


async def error_message(writer):
    writer.write(b'{"error": "malformed request"}\n')
    await writer.drain()


async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        try:
            data = await reader.readline()
            addr = writer.get_extra_info("peername")
            print(f"[{addr[0]}:{addr[1]}] {data.decode('utf-8').strip()}")
            if not data.endswith(b"\n"):
                await error_message(writer)
                break

            if not data:
                print(f"[{addr[0]}:{addr[1]}] closed")
                break
        except:
            break
        try:
            decoded = json.loads(data)
            if decoded["method"] != "isPrime":
                raise ValueError
            # :/ Booleans inherit from int in python
            if isinstance(decoded["number"], bool):
                await error_message("Not a valid number")
                break

            if not isinstance(decoded["number"], int):
                # May be a float
                if isinstance(decoded["number"], float):
                    writer.write(b'{"method": "isPrime", "prime": false}\n')
                    await writer.drain()
                    continue
                else:
                    await error_message(writer)
                    break
            if sympy.isprime(decoded["number"]):
                writer.write(b'{"method": "isPrime", "prime": true}\n')
                await writer.drain()
            else:
                writer.write(b'{"method": "isPrime", "prime": false}\n')
                await writer.drain()

        except Exception as e:
            await error_message(writer)
            break
    try:
        writer.close()
        await writer.wait_closed()
    except:
        pass


async def main():
    server = await asyncio.start_server(handle_request, "0.0.0.0", "8000")
    async with server:
        await server.serve_forever()


asyncio.run(main())
