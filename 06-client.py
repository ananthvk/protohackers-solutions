import asyncio


async def main():
    reader, writer = await asyncio.open_connection("127.0.0.1", 8000)
    writer.write(bytearray([0x80, 0x00, 0x7B, 0x00, 0x08, 0x00, 0x3C]))
    writer.write(
        bytearray([0x20, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x00, 0x00, 0x00])
    )
    writer.write(bytearray([0x81, 0x01, 0x00, 0x7B]))
    try:
        async with asyncio.timeout(3):
            message = await reader.read(4096)
        print(message[2:].decode("utf-8"))
    except:
        pass
    await writer.drain()
    writer.close()

    await writer.wait_closed()


asyncio.run(main())
