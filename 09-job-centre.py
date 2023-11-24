import jsonschema
import heapq
from jsonschema.exceptions import ValidationError
from typing import Any, Dict, Set, Tuple

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
job_id_counter = -1


class Job:
    def __init__(
        self, job_id: int, job: Dict[Any, Any], priority: int,  queue: str) -> None:
        self.job_id = job_id
        self.job = job
        self.queue = queue
        self.priority = priority
        self.running = False
        self.deleted = False

    @staticmethod
    def create(job: Dict[Any, Any], priority: int, queue: str) -> "Job":
        global job_id_counter  # Add this line
        job_id_counter += 1
        return Job(job_id_counter, job, priority, queue)

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



# Associates a queue name (string) with a heap(implemented as a list)
queues: Dict[str, list[Job]] = dict()

# Dictionary of jobs, job_id: job
jobs: Dict[int, Job] = dict()


def put(queue_name: str, job_dict: Dict[Any, Any], priority: int) -> Job:
    """
    Puts the job on the specified queue and returns the job object
    """

    if queue_name not in queues:
        # Create an empty job queue
        queues[queue_name] = []

    job = Job.create(job_dict, priority, queue_name)
    heapq.heappush(queues[queue_name], job)
    jobs[job.job_id] = job
    return job

def get(queues_list: list[str]):
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
        if queue in queues and queues[queue]:
            # Find the element with highest priority in this queue
            job = heapq.heappop(queues[queue])
            flag = False

            # Loop until the first non deleted job is found
            while job.deleted:
                if not queues[queue]: # The queue is empty
                    del queues[queue]
                    flag = True
                    break
                job = heapq.heappop(queues[queue])
            
            if flag:
                break

            if job.priority > highest_priority:
                highest_priority = job.priority
                highest_priority_job = job
            
            # Push the job back into the queue
            heapq.heappush(queues[queue], job)
    
    if highest_priority_job is None:
        return None

    return heapq.heappop(queues[highest_priority_job.queue])

def delete(job_id: int):
    """
    Returns True if the delete is valid
    False otherwise
    """
    # Check if the job has already been deleted
    if job_id not in jobs:
        return False
    
    # Check if the job id has not been allocated
    if job_id > job_id_counter:
        return False

    jobs[job_id].deleted = True
    del jobs[job_id]
    return True

def abort(job_id: int):
    # Check if the job has been deleted
    if job_id not in jobs:
        return False

    # Check if the job id has not been allocated
    if job_id > job_id_counter:
        return False
    
    job = jobs[job_id]
    job.running = False

    # Put the job back in its queue
    if job.queue not in queues:
        # Create an empty job queue
        queues[job.queue] = []
    heapq.heappush(queues[job.queue], job)
    return True

put("test", {"key1": "value1"}, 3)
put("test", {"key2": "value2"}, 2)
put("test", {"key3": "value3"}, 10)
put("test", {"key4": "value4"}, 6)
put("test1", {"keykk": "value1"}, 8)
put("test2", {"zzd": "mew"}, 20)
put("test2", {"zzds": "mew"}, 20)
put("nothing", {"cat": "meow"}, 5)
put("test", {"cat": "meow"}, 3)
print(queues)