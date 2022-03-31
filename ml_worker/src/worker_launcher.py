import json
import logging
import os
import time

import docker
import requests

from model import MlexWorker, Status


def init_logging():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)


def get_next_worker():
    '''
    This function returns the next worker in queue that matches the available compute resources at the host
    Returns:
        worker:     [MlexWorker]
    '''
    response = requests.get(f'{COMP_API_URL}workers', params={'host_uid': HOST_UID})
    worker = response.json()
    if worker:
        worker = MlexWorker.parse_obj(worker)
        logging.info(f'Found next worker: {worker.uid}')
    return worker


def update_worker_status(worker_id, status: Status):
    '''
    This function updates the worker status in the worker database
    Args:
        worker_id:  worker UID
        status:     [Status]
    Returns:
        None
    '''
    response = requests.patch(f'{COMP_API_URL}workers/{worker_id}/update', json=status.dict())
    logging.info(f'\"Update worker {worker_id} with status {status.state}\" {response.status_code}')
    pass


COMP_API_URL = 'http://job-service:8080/api/v0/private/'
HOST_UID = str(os.environ['HOST_UID'])
NUM_PROCESSORS = int(os.environ['NUM_PROCESSORS'])      # number of processors assigned to ml_workers
NETWORK = str(os.environ['NETWORK'])
DOCKER_CLIENT = docker.from_env()


if __name__ == '__main__':
    init_logging()
    while True:
        new_worker = get_next_worker()
        if new_worker:
            try:
                worker_info = json.dumps(new_worker.dict(), default=str)
                container = DOCKER_CLIENT.containers.run('mlexchange/dummy-worker',
                                                         cpu_count  = NUM_PROCESSORS,
                                                         command    = "python3 src/ml_worker.py \'"+ worker_info + ' \'',
                                                         network    = NETWORK,
                                                         volumes    = ["/var/run/docker.sock:/var/run/docker.sock"],
                                                         detach     = True)
            except Exception as err:
                logging.error(f'Worker {new_worker.uid} failed: {err}')
                status = Status(state="failed", return_code=err)
                update_worker_status(new_worker.uid, status)
        else:
            time.sleep(5)           # Idle for 5 seconds if no worker is found
