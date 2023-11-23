import jsonschema
import heapq
from jsonschema.exceptions import ValidationError
from typing import Any, Dict, Tuple

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

# This counter starts from one and is incremented after a job is added
# Since this application uses asyncio with single threading, there is no need to lock
# this counter.
job_id_counter = 0


class Job:
    def __init__(
        self, job_id: int, job: Dict[Any, Any], queue: str | None = None
    ) -> None:
        self.job_id = job_id
        self.job = job
        self.queue = queue

    @staticmethod
    def create(job: Dict[Any, Any], queue: str) -> "Job":
        global job_id_counter  # Add this line
        job_id_counter += 1
        return Job(job_id_counter, job, queue)

    def __str__(self) -> str:
        return f"< Job {self.job_id} of {self.queue}>"

    def __repr__(self) -> str:
        return self.__str__()


# Associates a queue name (string) with a heap(implemented as a list)
queues: Dict[str, list[Tuple[int, Job]]] = dict()


def put(queue_name: str, job_dict: Dict[Any, Any], priority: int) -> Job:
    """
    Puts the job on the specified queue and returns the job object
    """

    if queue_name not in queues:
        # Create an empty job queue
        queues[queue_name] = []

    job = Job.create(job_dict, queue_name)
    # Invert the priorities so that the default heap behaves as a max heap
    heapq.heappush(queues[queue_name], (-priority, job))
    return job

def get(queues_list: list[str]):
    """
    Returns the job with the highest priority among all the 
    given queues. If no job is found, None is returned
    """
    if queues_list is None:
        return None

    highest_priority = -1
    queue_with_hightest_priority = ""
    for queue in queues_list:
        if queue in queues:
            heap = queues[queue] 
            # Find the element with highest priority in this queue
            priority = -heap[0][0]
            queue_name = queue
            if priority > highest_priority:
                highest_priority = priority
                queue_with_hightest_priority = queue_name
    
    if highest_priority == -1:
        return None

    # We have found the queue which has the highest priority element
    # among the given queues.
    heap = queues[queue_with_hightest_priority]
    priority, job = heapq.heappop(heap)
    return job

put("test", {"key": "do this"}, 3)
put("test", {"key value": "GOOGOG"}, 2)
print(queues)
print(get(["test"]))