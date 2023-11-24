import os
import sys
import traceback

os.environ["PYTHONASYNCIODEBUG"] = "1"
import logging
import jsonschema
import asyncio
import heapq
from jsonschema.exceptions import ValidationError
from typing import Any, Dict, Set, Tuple
import coloredlogs

HOST = "0.0.0.0"
PORT = 8000

logger = logging.getLogger(name="job")
coloredlogs.install(
    level="DEBUG",
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
        # TODO: Check for deleted jobs

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

    def delete(self, job_id: int):
        """
        Returns True if the delete is valid
        False otherwise
        """
        # Check if the job has already been deleted
        if job_id not in self.jobs:
            return False

        # Check if the job id has not been allocated
        if job_id > self.job_id_counter:
            return False

        self.jobs[job_id].deleted = True
        del self.jobs[job_id]
        return True

    def abort(self, job_id: int):
        # Check if the job has been deleted
        if job_id not in self.jobs:
            return False

        # Check if the job id has not been allocated
        if job_id > self.job_id_counter:
            return False

        job = self.jobs[job_id]
        job.running = False

        # Put the job back in its queue
        if job.queue not in self.queues:
            # Create an empty job queue
            self.queues[job.queue] = []
        heapq.heappush(self.queues[job.queue], job)
        return True


async def client_accepted(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        address = writer.get_extra_info("peername")
        address = f'{address[0]}:{address[1]}'
        logger.info("%s connected", address)
        while True:
            data = await reader.readline()
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except:
        print("ERROR: ", traceback.format_exc())
    finally:
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
    server = await asyncio.start_server(client_accepted, host=HOST, port=PORT)
    logger.info("Started server on %s:%d", HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
