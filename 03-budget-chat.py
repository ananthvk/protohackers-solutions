import asyncio
import traceback
from datetime import datetime


def info(writer, *args, **kwargs):
    addr = writer.get_extra_info("peername")
    addr = f"{addr[0]}:{addr[1]}"
    print(f"{datetime.now().strftime('%T.%f')} [{addr}]", *args, **kwargs)


def log(writer, *args, **kwargs):
    return
    addr = writer.get_extra_info("peername")
    addr = f"{addr[0]}:{addr[1]}"
    print(f"{datetime.now().strftime('%T.%f')} [{addr}]", *args, **kwargs)


def log2(*args, **kwargs):
    print(f"{datetime.now().strftime('%T.%f')} [INFO]", *args, **kwargs)


background_tasks = {}


class Server:
    def __init__(
        self,
        address: str,
        port: str,
        timeout: int = 10,
    ) -> None:
        self.address = address
        self.port = port
        self.timeout = timeout
        self.connections = {}

    async def run(self):
        server = await asyncio.start_server(self.handle_client, self.address, self.port)
        log2("Started server on", f"{self.address}:{self.port}")

        async with server:
            await server.serve_forever()

    # Send the message to all connected clients, except the one with username
    async def send_all(self, username, message):
        message = message.encode("utf-8")
        for k in list(self.connections.keys()):
            v = self.connections[k]
            try:
                if k == username:
                    continue
                else:
                    v.write(message)
                    await v.drain()
            except Exception as e:
                # TODO: Check if write error occured, if so remove the client
                pass

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        username_accepted = False
        try:
            info(writer, "Client connected")
            writer.write(
                b"Hey, welcome to chatnet!, Send your username to start chatting: \n"
            )
            await writer.drain()
            log(writer, "Sent prompt to user")

            username = await reader.readline()
            if not username:
                return
            username = username.decode("utf-8").strip()
            log(writer, f"Got username={username} from user")

            if not username.isalnum():
                raise ValueError()
            # The username must not be empty
            if username == "":
                raise ValueError()
            # The username must contain atleast one character
            if not any(c.isalpha() for c in username):
                raise ValueError()
            # TODO: Check duplication
            log(writer, f"username={username} is valid")
            username_accepted = True

            log(writer, f"Sending join message to other users from {username}")
            await self.send_all(username, f"* {username} has joined the chat\n")
            log(writer, f"Sent join message from {username}")

            # First send a list of all users to the newly joined client
            log(writer, f"Sending list of users to {username}")
            writer.write(
                f'* The chat room has: {", ".join(self.connections.keys())}\n'.encode(
                    "utf-8"
                )
            )
            await writer.drain()
            log(writer, f"Sent list of users to {username}")
            self.connections[username] = writer
            while True:
                await self.handle_session(username, reader, writer)
        except Exception as e:
            # print(f'ERROR:{traceback.format_exc()}\n')
            if username_accepted:
                await self.send_all(username, f"* {username} has left the chat\n")
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
            finally:
                if username_accepted:
                    info(writer, f"{username} disconnected")
                    del self.connections[username]

    async def handle_session(
        self, username, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        data = await reader.readline()
        if not data:
            raise ValueError()
        message = data.decode("utf-8").strip()
        log(writer, f"Received message from {username}: {message}")
        await self.send_all(username, f"[{username}] {message}\n")
        log(writer, f"Sent received message from {username} to others: {message}")
        # writer.write(b'> ')
        # await writer.drain()


if __name__ == "__main__":
    server = Server("0.0.0.0", "8000", 30)
    asyncio.run(server.run())
