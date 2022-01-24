import coiled
import os
from dask.distributed import Client, LocalCluster
from distributed.diagnostics.plugin import UploadDirectory

import os

from external_fns.misc import get_prefix
from pipeline.functions.item_level import runner

get_prefix()

local = False # This script works if run on LocalCluster (local = True)

if __name__ == "__main__":

    if local:
        cluster = LocalCluster(n_workers=3)
        client = Client(cluster)

    else:
        # coiled.create_software_environment(
        #             name='jason-larsen/965',
        #             conda_env_name='base',
        #             conda='my_env.yml',
        #             force_rebuild=True
        #         )

        cluster = coiled.Cluster(
                    name='kelsey-test',
                    software='jason-larsen/965',
                    account='jason-larsen',
                    n_workers=1,
                    worker_cpu=1,
                    worker_class='distributed.Nanny'
                )
        client = Client(cluster)

        client.register_worker_plugin(UploadDirectory('pipeline'),
                                        nanny=True, update_path = True, restart = True)

    print("Created client")

    # See what the directory structure looks like
    def test_func():
        dirs = []
        for d in os.walk('dask-worker-space'):
            dirs.append(d)
        return dirs

    job = client.submit(test_func)
    print(job.result())
    #[('dask-worker-space', ['pipeline', 'worker-tpsqragy'], ['purge.lock', 'global.lock', 'worker-tpsqragy.dirlock']),
    # ('dask-worker-space/pipeline', ['functions'], ['__init__.py', 'errors.py']), 
    # ('dask-worker-space/pipeline/functions', [], ['__init__.py', 'item_level.py']), 
    # ('dask-worker-space/worker-tpsqragy', ['storage'], []), 
    # ('dask-worker-space/worker-tpsqragy/storage', [], [])]

    runner(client)

