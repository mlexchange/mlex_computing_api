import ast
import json
import logging
import os
import time

import docker
import requests

from model import MlexHost, MlexWorker, Status


def init_logging():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)


def get_host(host):
    '''
    This function retrieves the host UID. If the host does not exist, it submits the corresponding host and returns its
    UID
    Args:
        host:       Host information
    Returns:
        host_uid:   Host UID
    '''
    response = requests.get(f'{COMP_API_URL}hosts', params={'hostname': host.hostname})
    mlex_host = response.json()
    if not mlex_host:
        response = requests.post(f'{COMP_API_URL}hosts', json=host.dict())
        if response.status_code != 200:
            host_uid = -1
            logging.error(f'Could not submit new host {host.dict()}. \nError code: {response.status_code}')
        else:
            host_uid = response.json()
            logging.info(f'Created host with uid: {host_uid}')
    else:
        mlex_host = MlexHost.parse_obj(mlex_host)
        host_uid = mlex_host.uid
        logging.info(f'Found host with uid: {host_uid}')
    return host_uid


def get_next_worker(service_type, host_uid):
    '''
    This function returns the next worker in queue that matches the available compute resources at the host
    Args:
        host_uid:   Host UID
    Returns:
        worker:     [MlexWorker]
    '''
    response = requests.get(f'{COMP_API_URL}private/workers', params={'service_type': service_type,
                                                                      'host_uid': host_uid})
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
    response = requests.patch(f'{COMP_API_URL}private/workers/{worker_id}/update', json=status.dict())
    logging.info(f'\"Update worker {worker_id} with status {status.state}\" {response.status_code}')
    pass


COMP_API_URL = 'http://job-service:8080/api/v0/'
NUM_PROCESSORS = int(os.environ['NUM_PROCESSORS'])      # number of processors assigned to ml_workers
NETWORK = str(os.environ['NETWORK'])
HOST = ast.literal_eval(os.environ['HOST'])
DOCKER_CLIENT = docker.from_env()


if __name__ == '__main__':
    init_logging()                          # Init logging

    host = MlexHost.parse_obj(HOST)         # Get host UID
    host_uid = get_host(host)

    cont = -1
    while True and host_uid!=-1:
        cont += 1
        if cont < 5:                        # Priority to frontend services
            new_worker = get_next_worker('frontend', host_uid)
        elif cont < 7:                      # Next, hybrid services
            new_worker = get_next_worker('hybrid', host_uid)
        elif cont < 9:                      # Finally, backend services
            new_worker = get_next_worker('backend', host_uid)
        else:
            cont = -1
        if new_worker:
            try:
                worker_info = json.dumps(new_worker.dict(), default=str)
                container = DOCKER_CLIENT.containers.run('mlexchange/dummy-worker',
                                                         cpu_count  = NUM_PROCESSORS,
                                                         command    = "python3 src/ml_worker.py \'"+ worker_info+' \'',
                                                         network    = NETWORK,
                                                         volumes    = ["/var/run/docker.sock:/var/run/docker.sock"],
                                                         detach     = True)
            except Exception as err:
                logging.error(f'Worker {new_worker.uid} failed: {err}')
                status = Status(state="failed", return_code=err)
                update_worker_status(new_worker.uid, status)
        else:
            time.sleep(5)           # Idle for 5 seconds if no worker is found
