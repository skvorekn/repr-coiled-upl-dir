from dask.distributed import as_completed
from py_functions.errors import Error
from py_functions.misc import get_prefix

def runner(client):
    jobs = []
    for i in range(5):
        job = client.submit(combine)
        jobs.append(job)

    for future in as_completed(jobs):
        result = future.result()
        print(result)

def combine():
    return ['test','list']