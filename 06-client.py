import asyncio


async def main():
    creader, cwriter = await asyncio.open_connection("127.0.0.1", 8000)
    # Camera client
    cwriter.write(bytearray([0x80, 0x00, 0x7B, 0x00, 0x08, 0x00, 0x3C]))
    # Send a request for hearbeat
    cwriter.write(bytearray([0x40, 0x00, 0x00, 0x00, 0x0A]))
    # Send a plate to the server
    cwriter.write(
        bytearray([0x20, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x00, 0x00, 0x00])
    )
    # cwriter.write(bytearray([0x81, 0x01, 0x00, 0x7B]))
    await cwriter.drain()
    cwriter.close()
    await cwriter.wait_closed()

    c2reader, c2writer = await asyncio.open_connection("127.0.0.1", 8000)
    # Camera client
    c2writer.write(bytearray([0x80, 0x00, 0x7B, 0x00, 0x09, 0x00, 0x3C]))
    # Send a request for hearbeat
    c2writer.write(bytearray([0x40, 0x00, 0x00, 0x00, 0x0A]))
    # Send a plate to the server
    c2writer.write(
        bytearray([0x20, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x00, 0x00, 0x2d])
    )
    # c2writer.write(bytearray([0x81, 0x01, 0x00, 0x7B]))
    await c2writer.drain()
    c2writer.close()
    await c2writer.wait_closed()
    
    # Get a dispatcher
    dreader, dwriter = await asyncio.open_connection("127.0.0.1", 8000)
    # Dispatcher client
    dwriter.write(bytearray([0x81, 0x01, 0x00, 0x7b]))
    await asyncio.sleep(10)
    await dwriter.drain()
    dwriter.close()
    await dwriter.wait_closed()


asyncio.run(main())
