import json
import logging
import time

import docker
from pymongo import MongoClient
from starlette.config import Config

from model import SimpleJob


class DispatchService:
    def __init__(self, client, db_name=None):
        """ The service creates a dataset called job_list
        """
        if db_name is None:
            db_name = 'job_list'
        self._db = client[db_name]
        self._collection_job_list = self._db.job_list

    def find_job(self, uid=None):
        if uid:
            item = self._collection_job_list.find_one({"uid": uid})
        else:
            item = self._collection_job_list.find_one_and_update({"status": "sent_queue"},
                                                                 {'$set': {'status': "running"}})
        if item:
            item = self.clean_id(item)
            return SimpleJob.parse_obj(item)
        else:
            return None

    def update_status(self, uid, status, err=None):
        if err:
            self._collection_job_list.update_one(
                {'uid': uid},
                {'$set': {'status': status, 'error': err}})
        else:
            self._collection_job_list.update_one(
                {'uid': uid},
                {'$set': {'status': status}})

    def update_logs(self, uid, output):
        self._collection_job_list.update_one(
            {'uid': uid},
            {'$set': {'container_logs': output.decode('ascii')}})

    @staticmethod
    def clean_id(data):
        """ Removes mongo ID
        """
        if '_id' in data:
            del data['_id']
        return data


class Context:
    db: MongoClient = None
    job_svc: DispatchService = None


logger = logging.getLogger('job_manager')


def init_logging():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(JOB_MANAGER_LOG_LEVEL)


config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://mongodb:27017/job_list")
JOB_MANAGER_LOG_LEVEL = config("JOB_MANAGER_LOG_LEVEL", cast=str, default="INFO")

svc_context = Context

logger.debug('starting server')
db = MongoClient(MONGO_DB_URI)
worker_svc = DispatchService(db)
svc_context.job_svc = worker_svc

docker_client = docker.from_env()

if __name__ == '__main__':
    while True:
        new_job = svc_context.job_svc.find_job()

        if new_job:
            try:
                if new_job.container_kwargs['parameters']:
                    cmd = new_job.container_cmd + ' ' + ' '.join(new_job.container_kwargs['directories']) + ' \'' + \
                          str(json.dumps(new_job.container_kwargs['parameters'])) + '\''
                else:
                    cmd = new_job.container_cmd + ' ' + ' '.join(new_job.container_kwargs['directories'])
                container = docker_client.containers.run(new_job.container_uri,
                                                         command=cmd,
                                                         device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])],
                                                         volumes=['{}:/app/work/data'.format(new_job.data_uri)],
                                                         detach=True)
            except Exception as err:
                svc_context.job_svc.update_status(new_job.uid, "failed", repr(err))
            else:
                while container.status == 'created' or container.status == 'running':
                    new_job = svc_context.job_svc.find_job(new_job.uid)
                    if new_job.terminate:
                        container.kill()
                        svc_context.job_svc.update_status(new_job.uid, "terminated")
                    else:
                        try:
                            output = container.logs(stdout=True)
                            svc_context.job_svc.update_logs(new_job.uid, output)
                        except Exception as err:
                            svc_context.job_svc.update_status(new_job.uid, "failed", repr(err))
                    time.sleep(1)
                    container = docker_client.containers.get(container.id)
                result = container.wait()
                if result["StatusCode"] == 0:
                    output = container.logs(stdout=True)
                    svc_context.job_svc.update_logs(new_job.uid, output)
                    svc_context.job_svc.update_status(new_job.uid, "completed")
                else:
                    if new_job.terminate is None:
                        try:
                            output = container.logs(stdout=True)
                            svc_context.job_svc.update_logs(new_job.uid, output)
                        except Exception:
                            pass
                        err = "Code: "+str(result["StatusCode"])+ " Error: " + repr(result["Error"])
                        svc_context.job_svc.update_status(new_job.uid, "failed", err)
            # container.remove()
        else:
            # Idle for 5 seconds if no job is found
            time.sleep(5)
