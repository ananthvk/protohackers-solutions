import pytest
from job_centre import Job, JobManager

@pytest.fixture
def manager() -> JobManager:
    manager =  JobManager()
    manager.put("1", {"key": "value"}, 5)
    manager.put("1", {"key1": "value2"}, 7)
    manager.put("1", {"key1": "value2"}, 9)
    manager.put("1", {"ke": "val"}, 8)
    manager.put("2", {"keyc": "valued"}, 1)
    manager.put("2", {"python": "foo"}, 9)
    manager.put("3", {"keycv": "valueds"}, 15)
    manager.put("4", {"kkk": "vv"}, 16)
    manager.put("4", {"kkk": "vv"}, 14)
    return manager

def test_put():
    manager = JobManager()
    job = manager.put("q1", {"example": "testing"}, 5)
    assert(job.job_id == 0)

    job = manager.put("q1", {"example": "testing"}, 8)
    assert(job.job_id == 1)
    
    job = manager.put("q2", {"key": "value", "li": [1,2,3]}, 12)
    assert(job.job_id == 2)

def test_no_jobs_get():
    manager = JobManager()
    job = manager.get(["q2"])
    assert(job is None)
    job = manager.get(["1", "2", ""])
    assert(job is None)
    
def test_single_job_get():
    manager =JobManager()
    job = manager.put("q2", {"key": "value", "li": [1,2,3]}, 12)
    assert(job.job_id == 0)
    job = manager.get(["q2"])
    assert(job is not None)
    assert(job.job_id == 0)
    assert(not manager.queues['q2'])


def test_multiple_queues_get(manager: JobManager):
    assert(len(manager.queues['4']) == 2)
    job = manager.get(["1", "2", "3", "4"])
    assert(job is not None)
    assert(job.priority == 16)
    assert(len(manager.queues['4']) == 1)
    # Make sure that no other queues were modified
    assert(len(manager.queues['1']) == 4)
    assert(len(manager.queues['2']) == 2)
    assert(len(manager.queues['3']) == 1)
    job = manager.get(["3"])
    assert(job is not None)
    assert(job.priority == 15)
    assert(job.job['keycv'] == 'valueds')


def test_delete(manager: JobManager):
    assert(manager.delete(7))
    assert(not manager.delete(7))

    job = manager.get(["4"])
    assert(job is not None)
    assert(job.priority == 14)

def test_mutliple_deletes(manager: JobManager):
    assert(manager.delete(7))
    assert(manager.delete(8))
    
    job = manager.get(["4"])
    assert(job is None)
    assert(7 not in manager.jobs)
    assert(not manager.queues.get("4"))

def test_complete_delete(manager: JobManager):
    assert(not manager.delete(-1))
    assert(manager.delete(0))
    assert(manager.delete(1))
    assert(manager.delete(2))
    assert(manager.delete(3))
    assert(manager.delete(4))
    assert(manager.delete(5))
    assert(manager.delete(6))
    assert(manager.delete(7))
    assert(manager.delete(8))
    assert(not manager.delete(9))
    assert(not manager.jobs)
    # The items are lazy deleted
    for k, v in manager.queues.items():
        assert(v)
    # Perform get for all the ids, then check if all queues are empty
    assert(manager.get(["1", "2", "3", "4"]) is None)
    for k, v in manager.queues.items():
        assert(not v)

def test_abort(manager: JobManager):
    job = manager.get(["4"])
    assert(job is not None)
    assert(manager.abort(job.job_id))

    job2 = manager.get(["4"])
    assert(job is not None)
    assert(job == job2)

    manager.delete(7)
    assert(not manager.abort(job.job_id))
    job3 = manager.get(["4"])
    assert (job3 != job2)