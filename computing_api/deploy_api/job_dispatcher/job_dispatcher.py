import json
import logging
import subprocess
import time

import docker
from fastapi import FastAPI
from kafka import KafkaProducer
from kq import Queue
from kq.job import Job
import requests
from starlette.config import Config
import uvicorn


from model import SimpleJob, PatchRequest


logger = logging.getLogger('job_dispatcher')


def init_logging():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(JOB_DISPATCHER_LOG_LEVEL)


config = Config(".env")
KAFKA_URI = config("KAFKA_URI", cast=str, default="kafka:9092")
JOB_DISPATCHER_LOG_LEVEL = config("JOB_MANAGER_LOG_LEVEL", cast=str, default="INFO")

PRODUCER = KafkaProducer(bootstrap_servers=KAFKA_URI, api_version=(0, 9))
JOB_QUEUE = Queue(topic='submit-job', producer=PRODUCER)
DOCKER_CLIENT = docker.from_env()

API_URL_PREFIX = "/api/v0"

init_logging()

app = FastAPI(
    openapi_url="/api/job_dispatcher/openapi.json",
    docs_url="/api/job_dispatcher/docs",
    redoc_url="/api/job_dispatcher/redoc",)


@app.post(API_URL_PREFIX + '/jobs', tags=['jobs'])
def submit_job(job: SimpleJob):
    arg_dict = {'command': job.container_cmd,
                'kwargs': job.container_kwargs,
                'uid': job.uid}
    job = Job(id=job.uid,
              func=subprocess.run,
              args=[job.container_uri],
              kwargs={'command': 'python3 wrapper.py' + ' -d \'' + str(json.dumps(arg_dict)) + '\'',
                      'volumes': ['{}:/app/work/data'.format(job.data_uri)],
                      'detach': True})
    JOB_QUEUE.enqueue(job)
    return time.time()


@app.get(API_URL_PREFIX + '/jobs/output', tags=['jobs', 'output'])
def get_job_output(pid: str):
    container = DOCKER_CLIENT.containers.get(pid)
    output = container.logs(stdout=True)
    return output


@app.patch(API_URL_PREFIX + '/jobs/{uid}/status', tags=['jobs', 'status'])
def update_job_status(uid: str, req: PatchRequest):
    job_status = req.status
    job_pid = req.pid
    url = 'http://host.docker.internal:8080/api/v0/jobs/' + uid + '/status'  # ?status='+ job_status
    if job_pid is None:
        data = {'status': job_status}
    else:
        data = {'status': job_status, 'pid': str(job_pid)}
    return requests.patch(url, json=data).status_code


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8081)
