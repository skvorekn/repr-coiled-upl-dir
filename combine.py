import coiled
import os
from dask.distributed import Client, LocalCluster
from distributed.diagnostics.plugin import UploadDirectory
from tenacity import retry
import random
import string
import time

from py_functions.misc import get_prefix
from py_functions.pipeline.item_level import runner

get_prefix()

local = False
create_new_cluster = True

def update_path(dask_worker):
        import pathlib
        import sys
        path = str(pathlib.Path(dask_worker.local_directory).parent)
        if path not in sys.path:
            sys.path.insert(0, path)
        return sys.path

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

        @retry
        def create_cluster():
            random_string = ''.join([random.choice(string.ascii_lowercase) for i in range(10)])
            cluster = coiled.Cluster(
                        name=random_string,
                        software='jason-larsen/965',
                        account='jason-larsen',
                        n_workers=3,
                        worker_cpu=1,
                        worker_class='distributed.Nanny'
                    )
            return cluster

        if create_new_cluster:
            cluster = create_cluster()
        else:
            cluster = coiled.Cluster(name="ytdubbclaa")

        client = Client(cluster)
        time.sleep(10)

        path = client.run(update_path)
        print(path)

        client.register_worker_plugin(UploadDirectory('py_functions', update_path=False, restart=False),
                                     nanny=True)

    print("Created client")

    # See what the directory structure looks like
    def test_func():
        import sys
        return sys.path

    result = client.run(test_func)
    print(result)

    runner(client)
