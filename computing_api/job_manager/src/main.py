import logging
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.config import Config
import uvicorn

from model import SimpleJob, DeployLocation
from job_service import JobService, Context


logger = logging.getLogger('job_manager')


def init_logging():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(JOB_MANAGER_LOG_LEVEL)


config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://mongodb:27017/job_list")
JOB_MANAGER_DB_NAME = config("JOB_MANAGER_DB_NAME", cast=str, default="job_manager")
JOB_MANAGER_LOG_LEVEL = config("JOB_MANAGER_LOG_LEVEL", cast=str, default="INFO")


API_URL_PREFIX = "/api/v0"

init_logging()

app = FastAPI(
    openapi_url="/api/job_manager/openapi.json",
    docs_url="/api/job_manager/docs",
    redoc_url="/api/job_manager/redoc",)


svc_context = Context


@app.on_event("startup")
async def startup_event():
    from pymongo import MongoClient
    logger.debug('starting server')
    db = MongoClient(MONGO_DB_URI)
    job_svc = JobService(db)
    svc_context.job_svc = job_svc


class ResponseModel(BaseModel):
    uid: str


@app.post(API_URL_PREFIX + '/jobs', tags=['jobs'], response_model=ResponseModel)
def submit_job(job: SimpleJob):
    new_job = svc_context.job_svc.submit_job(job) #, JOB_QUEUE)
    return ResponseModel(uid=new_job.uid)


@app.get(API_URL_PREFIX + '/jobs', tags=['jobs'])
def get_job(
        user: Optional[str] = None,
        mlex_app: Optional[str] = None,
        job_type: Optional[str] = None,
        deploy_location: Optional[DeployLocation] = None
) -> List[SimpleJob]:
    """ This function returns the list of jobs that match the query parameters

    Args:
        user (Optional[str], optiona;): find jobs based on the user. Defaults to None
        mlex_app (Optional[str], optional): find jobs based on the app. Defaults to None
        job_type (Optional[str], optional): find jobs based on the job type. Defaults to None
        deploy_location (Optional[DeployLocation], optional): find jobs based on the
            deployment location. Defaults to None

    Returns:
        List[SimpleJob]: [Full object simplejobs that match the query parameters]
    """
    jobs = svc_context.job_svc.find_jobs(user=user, mlex_app=mlex_app, job_type=job_type, deploy_location=deploy_location)
    return jobs


@app.get(API_URL_PREFIX + '/jobs/{uid}/logs', tags=['jobs', 'logs'])
def get_job_logs(uid: str):
    output = svc_context.job_svc.get_logs(uid)
    return output


@app.patch(API_URL_PREFIX + '/jobs/{uid}/terminate', tags=['jobs', 'terminate'], response_model=ResponseModel)
def terminate_job(uid: str):
    svc_context.job_svc.terminate_job(uid)
    return ResponseModel(uid=uid)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)
