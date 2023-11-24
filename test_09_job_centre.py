import sys
import job_centre

def test_put():
    state = job_centre.State()
    job = job_centre.put(state, "q1", {"example": "testing"}, 5)
    assert(job.job_id == 0)

    job = job_centre.put(state, "q1", {"example": "testing"}, 8)
    assert(job.job_id == 1)
    
    job = job_centre.put(state, "q2", {"key": "value", "li": [1,2,3]}, 12)
    assert(job.job_id == 2)
