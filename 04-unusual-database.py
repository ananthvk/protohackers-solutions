import asyncio

store = {"version": "UDP KV Store v1.0"}


class UnusualDatabaseProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, client_address):
        request = data.decode()
        print(client_address, "sent", request)
        result = parse_request(request)
        if result:
            # Send the value for the given key
            self.transport.sendto(result.encode("utf-8"), client_address)


def parse_request(message):
    split = message.split("=", 1)
    if len(split) == 1:
        # It is a query for a key
        key = split[0]
        return f'{key}={store.get(key,"")}'
    else:
        # Insert the key
        # If the client tries to modify the version key, ignore it
        key, value = split
        if key != "version":
            store[key] = value
        return None


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    transport, protocol = loop.run_until_complete(
        loop.create_datagram_endpoint(
            lambda: UnusualDatabaseProtocol(), local_addr=("0.0.0.0", 8000)
        )
    )

    try:
        loop.run_forever()
    finally:
        transport.close()
        loop.close()


if __name__ == "__main__":
    main()
