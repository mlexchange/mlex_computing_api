from enum import Enum
from datetime import datetime

from pydantic import BaseModel, Extra, Field
from typing import Optional, List, Union


###################################################### DATA TYPES ######################################################
class States(str, Enum):
    queue = "queue"
    running = "running"
    warning= "warning"
    complete = "complete"
    complete_with_errors = "complete with errors"
    failed = "failed"
    canceled = "canceled"
    terminated = "terminated"


class WorkflowType(str, Enum):
    serial = "serial"
    parallel = "parallel"


###################################################### SUBCLASSES ######################################################
class Status(BaseModel):
    state: States
    return_code: Optional[str]


class TimeStamps(BaseModel):
    submission_time: datetime = Field(default_factory=datetime.utcnow)
    execution_time: Optional[datetime]
    end_time: Optional[datetime]


class DockerJob(BaseModel):
    uri: str = Field(description="container uri")
    type: str = 'docker'
    cmd: str = Field(description="command to run")
    # logs: Optional[str] = Field(description="container logs")
    kwargs: Optional[dict] = Field(description="container kwargs")


class Constraints(BaseModel):
    num_nodes: Optional[int] = Field(description="number of nodes")
    num_processors: Optional[int] = Field(description="number of processors per node")
    num_gpus: Optional[int] = Field(description="number of GPUs per node")


class WorkerRequirements(BaseModel):
    num_processors: Optional[int] = Field(description="number of processors per node")
    num_gpus: Optional[int] = Field(description="number of GPUs per node")
    list_gpus: Optional[List[int]] = []
    bank: Optional[str] = Field(description="bank/account")
    timeout: Optional[float] = Field(description="time limit in minutes")


class CompRequirements(WorkerRequirements):
    num_nodes: Optional[int] = Field(description="number of nodes")
    host_uid: Optional[str]
    constraints: Optional[List[Constraints]]


####################################################### CLASSES #######################################################
DEFAULT_TIMESTAMP = TimeStamps(submission_time=datetime.utcnow())
SCHEMA_VERSION = "1.0"
DEFAULT_UID = "425f6781-e42b-23e2-a341-2431564214523"
DEFAULT_JOB_PID = str(0)
DEFAULT_UID_LIST = [DEFAULT_UID]
DEFAULT_STATUS = Status(**{'state': 'queue'})
DEFAULT_LOGS = ''


class MlexHost(BaseModel):
    uid: str = DEFAULT_UID
    nickname: str = Field(description="host nickname")
    hostname: str = Field(description="remote host name")
    max_nodes: Optional[int] = Field(description="maximum number of nodes in host")
    max_processors: Optional[int] = Field(description="maximum number of processors in host")
    max_gpus: Optional[int] = Field(description="maximum number of GPUs in host")
    num_available_processors: Optional[int] = Field(description="number of available processors")
    num_available_gpus: Optional[int] = Field(description="number of available gpus")
    list_available_gpus: Optional[List[int]] = Field(description="list of available gpus")
    num_running_workers: Optional[int] = Field(description="number of running workers")


class BasicAsset(BaseModel):
    uid: str = DEFAULT_UID
    schema_version: str = SCHEMA_VERSION
    timestamps: TimeStamps = DEFAULT_TIMESTAMP
    error: Optional[str] = Field(description="error description")
    terminate: Optional[bool] = Field(description="terminate")


class MlexJob(BasicAsset):
    mlex_app: str = Field(description="MLExchange app associated with the job")
    description: Optional[dict] = Field(description="job description")
    job_kwargs: Union[DockerJob]
    working_directory: str = Field(description="dataset uri")
    status: Status = DEFAULT_STATUS
    pid: str = DEFAULT_JOB_PID
    logs: Optional[str]
    class Config:
        extra = Extra.ignore


class MlexWorker(BasicAsset):
    host_uid: str = Field(description='remote MLExchange host identifier')
    status: Status = DEFAULT_STATUS
    jobs_list: List[str] = DEFAULT_UID_LIST
    requirements: Optional[WorkerRequirements] = Field(description='computational requirements')
    class Config:
        extra = Extra.ignore


class MlexWorkflow(BasicAsset):
    user_uid: str = Field(description='user identifier')
    workflow_type: WorkflowType = Field(description='sequential vs parallel')
    workers_list: List[str] = DEFAULT_UID_LIST
    status: Status = DEFAULT_STATUS
    class Config:
        extra = Extra.ignore


class UserWorkflow(MlexWorkflow):
    job_list: List[MlexJob]
    requirements: Optional[CompRequirements] = Field(description='computational requirements')
