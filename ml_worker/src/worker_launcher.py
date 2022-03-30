import json
import time
import docker
import requests

from model import MlexWorker, Status


COMP_API_URL = 'http://job-service:8080/api/v0/private/'
HOST_UID = 'd077798e-d46b-4246-99ce-18ad9a46ab7b'
DOCKER_CLIENT = docker.from_env()


def get_next_worker():
    response = requests.get(f'{COMP_API_URL}workers', params={'host_uid': HOST_UID})
    worker = response.json()
    if worker:
        worker = MlexWorker.parse_obj(worker)
    return worker


def update_worker_status(worker_id, status: Status):
    response = requests.patch(f'{COMP_API_URL}workers/{worker_id}/update', json=status.dict())
    pass


if __name__ == '__main__':
    while True:
        new_worker = get_next_worker()
        if new_worker:
            try:
                worker_info = json.dumps(new_worker.dict(), default=str)
                container = DOCKER_CLIENT.containers.run('mlexchange/dummy-worker',
                                                         cpu_count = new_worker.requirements.num_processors,
                                                         # device_requests=[
                                                         #     docker.types.DeviceRequest(
                                                         #         device_ids=[new_worker.requirements.list_gpus],
                                                         #         capabilities=[['gpu']]
                                                         #     )
                                                         # ],
                                                         command = "python3 src/ml_worker.py \'"+ worker_info + ' \'',
                                                         network = 'mlex_api_default',
                                                         volumes=["/var/run/docker.sock:/var/run/docker.sock"],
                                                             #'{}:/app/work/data'.format(new_job.data_uri)],
                                                            # I need to add the data volume too
                                                         detach = True)
            except Exception as err:
                status = Status(state="failed", return_code=err)
                update_worker_status(new_worker.uid, status)
            else:
                status = Status(state="running")
                update_worker_status(new_worker.uid, status)
        else:
            # Idle for 5 seconds if no worker is found
            time.sleep(5)
