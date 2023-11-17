import asyncio
import time


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        try:
            message = await reader.read(4096)
            if not message:
                break
        except:
            break
        writer.write(message)
        await writer.drain()
    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", "8000")
    async with server:
        await server.serve_forever()


asyncio.run(main())
