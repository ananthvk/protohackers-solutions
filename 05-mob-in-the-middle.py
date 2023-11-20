# https://stackoverflow.com/questions/6713310/regex-specify-space-or-start-of-string-and-space-or-end-of-string
# (?:\s|^)[a-zA-Z0-9]{25,34}(?:\s|$)
# MITM
# Using asyncio, create a server which will handle requests from the clients
# For each client, open a connection to the chat server.
# When the client sends a message, using the above regex, substitute the address with the fake one
# and then pass it on to the server.
# Do the same when the server sends a message, modify the message, then send it to the client
import asyncio
from datetime import datetime
import re

remote_host = "chat.protohackers.com"
remote_port = 16963
# remote_host = "127.0.0.1"
# remote_port = 8001
tony_address = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
# There must be a better way to do this :(
pattern1 = re.compile(r"(?<=\s)7[a-zA-Z0-9]{25,34}(?=\s)")
pattern2 = re.compile(r"(?<=\s)7[a-zA-Z0-9]{25,34}$")
pattern3 = re.compile(r"^7[a-zA-Z0-9]{25,34}(?=\s)")


def modify_message(data):
    data = data.decode("utf-8")
    data = pattern1.sub(tony_address, data)
    data = pattern2.sub(tony_address, data)
    data = pattern3.sub(tony_address, data)
    return data.encode("utf-8")


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


class Server:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.tasks = set()
        self.tasks_addr = dict()

    async def accept_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            log(writer, "1 ] Started man in the middle proxy for this client")
            sreader, swriter = await asyncio.open_connection(remote_host, remote_port)
            # Get the initial message from the server and pass it to the client
            log(writer, "2 ] Opened a connection to remote")
            message = await sreader.readline()
            log(writer, "3 ] Received welcome message from remote")
            writer.write(message)
            await writer.drain()
            log(writer, "3*] Sent welcome message to client")

            # Username
            username = await reader.readline()
            log(writer, f"4 ] Got username={username} from client")
            swriter.write(username)
            await swriter.drain()
            log(writer, f"4*] Sent username={username} to remote")

            # Send message from server to client
            welcome = await sreader.readline()
            log(writer, "5 ] Got list of users from remote")
            writer.write(welcome)
            await writer.drain()
            log(writer, "5*] Sent list of users to client")

            log(writer, "6 ] Creating handle_client_write")
            # Pass writer so that log can log the client address
            task1 = self.loop.create_task(
                self.handle_client_write(writer, reader, swriter)
            )
            self.tasks_addr[writer.get_extra_info("peername")] = [task1]
            log(writer, "6*] Created handle_client_write")
            self.tasks.add(task1)
            log(writer, "6#] Added task to set")
            task1.add_done_callback(self.tasks.discard)
            log(writer, "6&] Added task callback")

            log(writer, "7 ] Creating handle_server_write")
            task2 = self.loop.create_task(
                self.handle_server_write(swriter, sreader, writer)
            )
            self.tasks_addr[writer.get_extra_info("peername")] += [task2]
            log(writer, "7*] Created handle_server_write")
            self.tasks.add(task2)
            log(writer, "7#] Added task to set")
            task2.add_done_callback(self.tasks.discard)
            log(writer, "7&] Added task callback")
        except:
            try:
                if "swriter" in locals():
                    log(writer, "8 ] Closing connection to server")
                    swriter.close()
                    await swriter.wait_closed()
                    log(writer, "8*] Closed connection to server")
            except:
                pass

            try:
                log(writer, "8 ] Closing connection to client")
                writer.close()
                await writer.wait_closed()  # Get a message from the client, pass it to the server
                log(writer, "8*] Closed connection to client")
            except:
                pass

    async def handle_client_write(self, writer, reader, swriter):
        # Get a message from the client, pass it to the server
        # TODO: Do the same task removal for handle_server_write also
        try:
            log(writer, "Started handle_client_write")
            while True:
                log(writer, "Waiting for client to send data")
                data = await reader.readline()
                data = modify_message(data)
                log(writer, f"Got message from client: {data}")
                if not data:
                    log(writer, f"Client disconnected")
                    # Client disconnected
                    break
                log(writer, f"Sending data {data} to server")
                swriter.write(data)
                await swriter.drain()
                log(writer, f"Sent data to server")
            log(writer, f"Exiting handle_client_write")
        finally:
            try:
                servtask = self.tasks_addr[writer.get_extra_info("peername")][1]
                servtask.cancel()
            except:
                pass
            try:
                log(writer, "c ] Closing connection to server")
                swriter.close()
                await swriter.wait_closed()
                log(writer, "c*] Closed connection to server")
            except:
                pass

            try:
                log(writer, "c ] Closing connection to client")
                writer.close()
                await writer.close()  # Get a message from the client, pass it to the server
                log(writer, "c*] Closed connection to client")
            except:
                pass

    async def handle_server_write(self, swriter, sreader, writer):
        # Get the data from the server and after modifying, pass it on the client
        # TODO, if this task exits, close the other task also
        try:
            log(writer, "Started handle_server_write")
            while True:
                log(writer, "Waiting for server to send data")
                data = await sreader.readline()
                data = modify_message(data)
                log(writer, f"Got message from server: {data}")
                if not data:
                    log(writer, f"Server disconnected")
                    # Server disconnected
                    break
                log(writer, f"Sending data {data} to client")
                writer.write(data)
                await writer.drain()
                log(writer, f"Sent data to client")
            log(writer, f"Exiting handle_server_write")
        finally:
            try:
                clienttask = self.tasks_addr[writer.get_extra_info("peername")][0]
                clienttask.cancel()
            except:
                pass
            try:
                log(writer, "s ] Closing connection to server")
                swriter.close()
                await swriter.wait_closed()
                log(writer, "s*] Closed connection to server")
            except:
                pass

            try:
                log(writer, "s ] Closing connection to client")
                writer.close()
                await writer.close()  # Get a message from the client, pass it to the server
                log(writer, "s*] Closed connection to client")
            except:
                pass


async def main():
    loop = asyncio.get_running_loop()
    mitm = Server(loop)
    server = await asyncio.start_server(mitm.accept_client, host="0.0.0.0", port=8000)
    log2("Started server on 0.0.0.0:8000")
    async with server:
        await server.serve_forever()


asyncio.run(main())
