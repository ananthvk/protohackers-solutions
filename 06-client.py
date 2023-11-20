import asyncio


async def main():
    creader, cwriter = await asyncio.open_connection("127.0.0.1", 8000)
    # Camera client
    cwriter.write(bytearray([0x80, 0x00, 0x7B, 0x00, 0x08, 0x00, 0x3C]))
    # Send a plate to the server
    cwriter.write(bytearray([0x40, 0x00, 0x00, 0x00, 0x0A]))
    while True:
        message = await creader.read(4096)
        print(message)

    # cwriter.write(
    #    bytearray([0x20, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x00, 0x00, 0x00])
    # )
    # cwriter.write(bytearray([0x81, 0x01, 0x00, 0x7B]))
    try:
        async with asyncio.timeout(1):
            message = await creader.read(4096)
        print(message[2:].decode("utf-8"))
    except:
        pass
    await cwriter.drain()
    cwriter.close()

    await cwriter.wait_closed()


asyncio.run(main())
