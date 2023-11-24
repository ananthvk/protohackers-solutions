import os
import json

os.environ["PYTHONASYNCIODEBUG"] = "1"
import logging
# import jsonschema
# from jsonschema.exceptions import ValidationError
import asyncio
import heapq
import fastjsonschema 
from typing import Any, Dict, Set, Tuple
import coloredlogs

HOST = "0.0.0.0"
PORT = 8000

logger = logging.getLogger(name="job")
coloredlogs.install(
    level="CRITICAL",
    # logger=logger, # Enable this to only pass logs from this logger instance
    milliseconds=True,
    fmt="%(asctime)s %(name)-8s [%(levelname)-9s] %(funcName)-8s %(message)s",
)

put_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "PUT request",
    "description": "A client has sent a request for a job to be added",
    "properties": {
        "request": {"type": "string"},
        "queue": {"type": "string"},
        "job": {"type": "object"},
        "pri": {"type": "integer", "minimum": 0},
    },
    "required": ["request", "queue", "job", "pri"],
    "additionalProperties": False,
}

get_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "GET request",
    "description": "A client has requested a job from the specified queues",
    "properties": {
        "request": {"type": "string"},
        "queues": {"type": "array", "items": {"type": "string"}, "minItems": 1},
        "wait": {"type": "boolean"},
    },
    "required": ["request", "queues"],
    "additionalProperties": False,
}

abort_delete_schema = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "title": "ABORT/DELETE request",
    "description": "A client has requested a job to be removed or aborted",
    "properties": {
        "request": {"type": "string", "enum": ["abort", "delete"]},
        "id": {"type": "integer", "minimum": 0},
    },
    "required": ["request", "id"],
    "additionalProperties": False,
}

request_schema = {
    "type": "object",
    "title": "Request",
    "description": "Can be any request type",
    "properties": {
        "request": {"type": "string", "enum": ["get", "put", "abort", "delete"]},
    },
    "required": ["request"],
}

put_schema_validator = fastjsonschema.compile(put_schema)
get_schema_validator = fastjsonschema.compile(get_schema)
abort_delete_schema_validator = fastjsonschema.compile(abort_delete_schema)
request_schema_validator = fastjsonschema.compile(request_schema)

class Job:
    def __init__(
        self, job_id: int, job: Dict[Any, Any], priority: int, queue: str
    ) -> None:
        self.job_id = job_id
        self.job = job
        self.queue = queue
        self.priority = priority
        self.running = False
        self.deleted = False

    def __str__(self) -> str:
        return f"< Job {self.job_id} of {self.queue} [{self.priority}]>"

    def __repr__(self) -> str:
        return self.__str__()

    def __lt__(self, other):
        """
        Returns true if the priority of this job is higher than other
        It is defined for less than(<) so that these objects behave can be
        added to the heap to make it a MaxHeap
        """
        return self.priority > other.priority


class JobManager:
    """
    Manages jobs and their queues
    """

    def __init__(self) -> None:
        # Associates a queue name (string) with a heap(implemented as a list)
        self.queues: Dict[str, list[Job]] = dict()

        # Dictionary of jobs, job_id: job
        self.jobs: Dict[int, Job] = dict()

        # This counter starts from one and is incremented after a job is added
        self.job_id_counter = -1

    def _create_job(self, job: Dict[Any, Any], priority: int, queue: str) -> Job:
        self.job_id_counter += 1
        return Job(self.job_id_counter, job, priority, queue)

    def put(self, queue_name: str, job_dict: Dict[Any, Any], priority: int) -> Job:
        """
        Puts the job on the specified queue and returns the job object
        """

        if queue_name not in self.queues:
            # Create an empty job queue
            self.queues[queue_name] = []

        job = self._create_job(job_dict, priority, queue_name)
        heapq.heappush(self.queues[queue_name], job)
        self.jobs[job.job_id] = job
        return job

    def get(self, queues_list: list[str]):
        """
        Returns the job with the highest priority among all the
        given queues. If no job is found, None is returned
        """

        if queues_list is None:
            return None

        highest_priority: int = -1
        highest_priority_job: Job | None = None
        for queue in queues_list:
            # Check if the queue exists and is not empty
            if queue in self.queues and self.queues[queue]:
                # Find the element with highest priority in this queue
                job = heapq.heappop(self.queues[queue])
                flag = False

                # Loop until the first non deleted job is found
                while job.deleted:
                    if not self.queues[queue]:  # The queue is empty
                        # del self.queues[queue]
                        flag = True
                        break
                    job = heapq.heappop(self.queues[queue])

                if flag:
                    continue

                if job.priority > highest_priority:
                    highest_priority = job.priority
                    highest_priority_job = job

                # Push the job back into the queue
                heapq.heappush(self.queues[queue], job)

        if highest_priority_job is None:
            return None

        return heapq.heappop(self.queues[highest_priority_job.queue])
    
    def is_valid(self, job_id: int):
        """
        Returns True if this is a valid job id
        """
        # Check if the job has been deleted
        if job_id not in self.jobs:
            return False

        # Check if the job id has not been allocated
        if job_id > self.job_id_counter:
            return False
        return True

    def delete(self, job_id: int):
        """
        Returns True if the delete is valid
        False otherwise
        """
        if not self.is_valid(job_id):
            return False
        self.jobs[job_id].deleted = True
        del self.jobs[job_id]
        return True

    def abort(self, job_id: int):
        if not self.is_valid(job_id):
            return False

        job = self.jobs[job_id]
        job.running = False

        # Put the job back in its queue
        if job.queue not in self.queues:
            # Create an empty job queue
            self.queues[job.queue] = []
        heapq.heappush(self.queues[job.queue], job)
        return True


class Server:
    def __init__(self) -> None:
        # Dictionary of a client address with the jobs it has requested using GET
        self.clients: Dict[str, set] = dict()
        self.job_manager = JobManager()

    async def handle_put(
        self, address: str, writer: asyncio.StreamWriter, instance: Dict[Any, Any]
    ):
        put_schema_validator(instance) # type: ignore
        job = self.job_manager.put(instance["queue"], instance["job"], instance["pri"])
        writer.write(b'{"status":"ok","id":%d}\n' % job.job_id)

    async def handle_get(
        self, address: str, writer: asyncio.StreamWriter, instance: Dict[Any, Any]
    ):
        get_schema_validator(instance) #type: ignore
        job = self.job_manager.get(instance["queues"])
        if job is not None:
            self.clients[address].add(job.job_id)
            job_as_json = json.dumps(job.job).encode("utf-8")
            writer.write(
                b'{"status":"ok","id":%d,"job":%s,"pri":%d,"queue":"%s"}\n'
                % (job.job_id, job_as_json, job.priority, job.queue.encode("utf-8"))
            )
            return

        if instance.get('wait', False):
            # Crude solution, poll every 1 second to see if a new job has been added
            # A better way is to keep a list of waiters, and when a new job is inserted
            # If a waiter is present, send it to the waiter instead of adding it to the
            # queue
            while job is None:
                job = self.job_manager.get(instance["queues"])
                await asyncio.sleep(0.5)
            self.clients[address].add(job.job_id)
            job_as_json = json.dumps(job.job).encode("utf-8")
            writer.write(
                b'{"status":"ok","id":%d,"job":%s,"pri":%d,"queue":"%s"}\n'
                % (job.job_id, job_as_json, job.priority, job.queue.encode("utf-8"))
            )
        else:
            writer.write(b'{"status":"no-job"}\n')

    async def handle_delete(
        self, address: str, writer: asyncio.StreamWriter, instance: Dict[Any, Any]
    ):
        abort_delete_schema_validator(instance) #type: ignore
        status = self.job_manager.delete(instance["id"])
        if not status:
            writer.write(b'{"status":"no-job"}\n')
        else:
            writer.write(b'{"status":"ok"}\n')

    async def handle_abort(
        self, address: str, writer: asyncio.StreamWriter, instance: Dict[Any, Any]
    ):
        abort_delete_schema_validator(instance) #type: ignore
        # Check if this is a valid job id
        if not self.job_manager.is_valid(instance["id"]):
            writer.write(b'{"status":"no-job"}\n')
            return

        if instance["id"] not in self.clients[address]:
            writer.write(
                b'{"status": "error", "error": "Only the client which requested this job can abort it"}\n'
            )
            return

        status = self.job_manager.abort(instance["id"])
        if not status:
            writer.write(b'{"status":"no-job"}\n')
        else:
            writer.write(b'{"status":"ok"}\n')

    async def accept_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        address = ""
        try:
            address = writer.get_extra_info("peername")
            address = f"{address[0]}:{address[1]}"
            logger.info("%s connected", address)
            self.clients[address] = set()
            while True:
                if not (await self.process_client(address, reader, writer)):
                    break
        except:
            logger.exception("%s error occured while handling client", address)
        finally:
            await self.close_client(address, writer)

    async def process_client(
        self, address: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        data = await reader.readline()
        if not data:
            logger.info("%s disconnected", address)
            return False
        logger.info("%s sent %s", address, data)
        # Load and validate the data
        try:
            instance = json.loads(data)
            request_schema_validator(instance) #type: ignore
            # Instance is a valid request and has a request field
            if instance["request"] == "get":
                await self.handle_get(address, writer, instance)
            elif instance["request"] == "put":
                await self.handle_put(address, writer, instance)
            elif instance["request"] == "delete":
                await self.handle_delete(address, writer, instance)
            else:
                await self.handle_abort(address, writer, instance)
        except fastjsonschema.JsonSchemaValueException as e:
            logger.warning("%s invalid request: %s", address, e.message)
            writer.write(
                b'{"status": "error", "error": "%s"}\n' % e.message.encode("utf-8")
            )
        except json.JSONDecodeError as e:
            logger.warning("%s illformated json: %s", address, e.msg)
            writer.write(
                b'{"status": "error", "error": "%s"}\n' % e.msg.encode("utf-8")
            )
        except Exception as e:
            logger.critical("%s unhandled exception", address, exc_info=1)  # type: ignore
            writer.write(b'{"status": "error", "error": "unhandled exception"}\n')

        await writer.drain()
        return True

    async def close_client(self, address: str, writer: asyncio.StreamWriter):
        try:
            logger.info("%s clearing all jobs of this client", address)
            # When the client disconnects, abort all running jobs of this client
            for job in self.clients[address]:
                self.job_manager.abort(job)
            del self.clients[address]
        except:
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass


async def main():
    loop = asyncio.get_running_loop()

    # Set asyncio debug
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    logger.info("This is from logger")
    job_server = Server()
    server = await asyncio.start_server(job_server.accept_client, host=HOST, port=PORT)
    logger.info("Started server on %s:%d", HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

# TODO: Implement wait in get request, refactor the requests to reduce that many if-elses
# TODO: Associate a client with abort and get requests
