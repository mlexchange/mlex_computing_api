import logging
import os
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.config import Config
import uvicorn

from model import MlexHost, MlexJob, MlexWorker, MlexWorkflow, UserWorkflow, Status
from job_service import ComputeService, Context


logger = logging.getLogger('job_manager')


def init_logging():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(JOB_MANAGER_LOG_LEVEL)


config = Config(".env")
JOB_MANAGER_DB_NAME = config("JOB_MANAGER_DB_NAME", cast=str, default="job_manager")
JOB_MANAGER_LOG_LEVEL = config("JOB_MANAGER_LOG_LEVEL", cast=str, default="INFO")
MONGO_DB_USERNAME = str(os.environ['MONGO_INITDB_ROOT_USERNAME'])
MONGO_DB_PASSWORD = str(os.environ['MONGO_INITDB_ROOT_PASSWORD'])
MONGO_DB_URI = "mongodb://%s:%s@mongodb:27017/?authSource=admin" % (MONGO_DB_USERNAME, MONGO_DB_PASSWORD)

API_URL_PREFIX = "/api/v0"

init_logging()

app = FastAPI(
    openapi_url="/api/job_manager/openapi.json",
    docs_url="/api/job_manager/docs",
    redoc_url="/api/job_manager/redoc",)


svc_context = Context


@app.on_event("startup")
def startup_event():
    from pymongo import MongoClient
    logger.debug('starting server')
    db = MongoClient(MONGO_DB_URI)
    comp_svc = ComputeService(db)
    svc_context.comp_svc = comp_svc


def set_compute_service(new_comp_svc: ComputeService):
    global comp_svc
    svc_context.comp_svc = new_comp_svc


class ResponseModel(BaseModel):
    uid: str


@app.post(API_URL_PREFIX + '/workflows', tags=['workflows'])
def submit_workflow(workflow: UserWorkflow):
    '''
    This function submits a new workflow to queue
    Args:
        workflow: workflow with list of jobs to execute
    Returns:
        workflow_uid if the workflow is valid, -1 if invalid
    '''
    new_workflow_uid = svc_context.comp_svc.submit_workflow(workflow=workflow)
    return new_workflow_uid


@app.post(API_URL_PREFIX + '/hosts', tags=['hosts'])
def submit_host(host: MlexHost):
    '''
    This function submits a new host to MLExchange
    Args:
        host: host details
    Returns:
        host_uid
    '''
    new_host_uid = svc_context.comp_svc.submit_host(host=host)
    return new_host_uid


@app.get(API_URL_PREFIX + '/hosts', tags=['hosts'])
def get_host(host_uid: str = None,
             hostname: str = None,
             nickname: str = None):
    '''
    This function requests the list of resources from user portal
    Args:
        host_uid
        hostname
        nickname
    Returns:
        List of resources at host
    '''
    output = svc_context.comp_svc.get_host(host_uid=host_uid, hostname=hostname, nickname=nickname)
    return output


@app.get(API_URL_PREFIX + '/workflows/{uid}', tags=['workflows'])
def get_workflow(uid: str) -> MlexWorkflow:
    """
    This function returns the workflow that matches the query parameters
    Args:
        uid:            workflow uid
    Returns:
        MlexWorkflow: Full object MlexWorkflow that matches the query parameters
    """
    workflow = svc_context.comp_svc.get_workflow(uid=uid)
    return workflow


@app.get(API_URL_PREFIX + '/workflows', tags=['workflows'])
def get_workflows(user: Optional[str] = None,
                  host_uid: Optional[str] = None
                  ) -> List[MlexWorkflow]:
    """
    This function returns the list of jobs that match the query parameters
    Args:
        user (Optional[str], optional): find workflows based on the user. Defaults to None
        host_uid (Optional[str], optional): find workflows based on the host uid. Defaults to None
    Returns:
        List[MlexWorkflow]: [Full object MlexWorkflow that match the query parameters]
    """
    workflows = svc_context.comp_svc.get_workflows(user=user, host_uid=host_uid)
    return workflows


@app.get(API_URL_PREFIX + '/workers/{uid}', tags=['workers'])
def get_worker(uid: str) -> MlexWorker:
    '''
    This function returns the worker that matches the query parameters
    Args:
        uid:       Worker uid
    Returns:
        Worker
    '''
    worker = svc_context.comp_svc.get_worker(uid=uid)
    return worker


@app.get(API_URL_PREFIX + '/workers', tags=['workers'])
def get_workers(host_uid: Optional[str] = None,
                status: Optional[Status] = None
                ) -> List[MlexWorker]:
    '''
    This function returns the information on the user
    Args:
        host_uid:       Host uid
        status:         Worker status
    Returns:
        Worker information
    '''
    workers = svc_context.comp_svc.get_workers(host_uid=host_uid, status=status)
    return workers


@app.get(API_URL_PREFIX + '/jobs/{uid}', tags=['jobs'])
def get_job(uid: str) -> MlexJob:
    """
    This function returns the job that matches the query parameters
    Args:
        uid:    Job UID
    Returns:
        MlexJob: Full object MlexJob that matches the query parameters
    """
    job = svc_context.comp_svc.get_job(uid=uid)
    return job


@app.get(API_URL_PREFIX + '/jobs', tags=['jobs'])
def get_jobs(user: Optional[str] = None,
             mlex_app: Optional[str] = None,
             host_uid: Optional[str] = None,
             status: Optional[Status] = None
             ) -> List[MlexJob]:
    """
    This function returns the list of jobs that match the query parameters
    Args:
        user (Optional[str], optional): find jobs based on the user. Defaults to None
        mlex_app (Optional[str], optional): find jobs based on the app that launched the workflow. Defaults to None
        host_uid (Optional[str], optional): find jobs based on the host uid. Defaults to None
        status (Optional[Status], optional): find jobs based on the status. Defaults to None
    Returns:
        List[MlexJob]: [Full object MlexJob that match the query parameters]
    """
    jobs = svc_context.comp_svc.get_jobs(user=user, mlex_app=mlex_app, host_uid=host_uid, status=status)
    return jobs


@app.get(API_URL_PREFIX + '/private/jobs/{uid}', tags=['private'])
def get_next_job(uid: str) -> MlexJob:
    """
    This function returns the job that matches the query parameters
    Args:
        uid:    Job UID
    Returns:
        MlexJob: Full object MlexJob that matches the query parameters
    """
    job = svc_context.comp_svc.get_next_job(uid=uid)
    return job


@app.get(API_URL_PREFIX + '/private/workers', tags=['private'])
def get_next_worker(service_type: str, host_uid: str = None) -> MlexWorker:
    '''
    This function returns the next worker to be launched at host location and updates the status of this worker and the
    host resources in the database
    Args:
        host_uid:       Host uid
        service_type:   Frontend, Backend, Hybrid
    Returns:
        Worker to be executed
    '''
    next_worker = svc_context.comp_svc.get_next_worker(host_uid, service_type)
    return next_worker


@app.patch(API_URL_PREFIX + '/workflows/{uid}/terminate', tags=['workflows'], response_model=ResponseModel)
def terminate_workflow(uid: str):
    '''
    This function terminates the workflow
    Args:
        uid: Unique workflow identifier
    Returns:
        workflow_uid
    '''
    svc_context.comp_svc.terminate_workflow(uid)
    return ResponseModel(uid=uid)


@app.patch(API_URL_PREFIX + '/private/workers/{uid}/update', tags=['private'], response_model=ResponseModel)
def update_worker(uid: str,
                  status: Status
                  ):
    '''
    This function updates the worker status
    Args:
        uid: Unique worker identifier
        status:     Worker status
    Returns:
        worker_uid
    '''
    svc_context.comp_svc.update_worker(uid, status)
    return ResponseModel(uid=uid)


@app.patch(API_URL_PREFIX + '/workers/{uid}/terminate', tags=['workers'], response_model=ResponseModel)
def terminate_worker(uid: str):
    '''
    This function terminates the worker operation
    Args:
        uid: Unique worker identifier
    Returns:
        worker_uid
    '''
    svc_context.comp_svc.terminate_worker(uid)
    return ResponseModel(uid=uid)


@app.patch(API_URL_PREFIX + '/private/jobs/{uid}/update', tags=['private'], response_model=ResponseModel)
def update_job(uid: str,
               status: Optional[Status] = None,
               logs: Optional[str] = None
               ):
    '''
    This function updates the job status
    Args:
        uid:        Unique job identifier
        status:     Job status
        logs:       Job logs
    Returns:
        job_uid
    '''
    svc_context.comp_svc.update_job(uid, status, logs)
    return ResponseModel(uid=uid)


@app.patch(API_URL_PREFIX + '/jobs/{uid}/terminate', tags=['jobs'], response_model=ResponseModel)
def terminate_job(uid: str):
    '''
    This function terminates the job
    Args:
        uid: Unique job identifier
    Returns:
        job_uid
    '''
    svc_context.comp_svc.terminate_job(uid)
    return ResponseModel(uid=uid)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)
