from dask.distributed import as_completed
from py_functions.errors import Error
from py_functions.misc import get_prefix
from tenacity import retry

def runner(client, n_workers):
    admin_ids = range(5)
    num_not_started_admin_ids = len(admin_ids)
    processed_count = 0
    jobs = []

    while processed_count < num_not_started_admin_ids:
        while len(jobs) < n_workers: 

            job = client.submit(combine)
            jobs.append(job)

        for future in as_completed(jobs):
            processed_count += 1
            jobs.remove(future)

            result = future.result()
            print(result)

@retry
def combine():
    get_prefix()
    return ['test','list']