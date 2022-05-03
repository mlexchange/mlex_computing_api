from enum import Enum
from datetime import datetime

from pydantic import BaseModel, Extra, Field, validator
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


class ServiceType(str, Enum):
    frontend = "frontend"
    backend = "backend"
    hybrid = "hybrid"


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
    port: Optional[List[int]] = Field(description="port to expose")
    kwargs: Optional[dict] = Field(description="container kwargs")


class Resources(BaseModel):
    num_processors: Optional[int] = Field(description="number of processors per node")
    num_gpus: Optional[int] = Field(description="number of GPUs per node")
    list_gpus: Optional[List[str]] = []


class Constraints(Resources):
    num_nodes: Optional[int] = Field(description="number of nodes")


class WorkerRequirements(Resources):
    bank: Optional[str] = Field(description="bank/account")
    timeout: Optional[float] = Field(description="time limit in minutes")
    kwargs: Optional[dict]


class CompRequirements(WorkerRequirements):
    num_nodes: Optional[int] = Field(description="number of nodes")
    host_uid: Optional[str]
    constraints: Optional[List[Constraints]]

class ResourcesQuery(Resources):
    service_type: ServiceType


####################################################### CLASSES #######################################################
DEFAULT_TIMESTAMP = TimeStamps(submission_time=datetime.utcnow())
SCHEMA_VERSION = "1.0"
DEFAULT_UID = "425f6781-e42b-23e2-a341-2431564214523"
DEFAULT_JOB_PID = str(0)
DEFAULT_UID_LIST = [DEFAULT_UID]
DEFAULT_STATUS = Status(**{'state': 'queue'})
DEFAULT_LOGS = ''
DEFAULT_SERVICE = 'hybrid'
DEFAULT_CONSTRAINTS = {'num_processors': 0,
                       'num_gpus': 0,
                       'list_gpus': [],
                       'num_nodes': 0}


class MlexHost(BaseModel):
    uid: str = DEFAULT_UID
    nickname: str = Field(description="host nickname")
    hostname: str = Field(description="remote host name")
    frontend_constraints: Constraints           # maximum resources
    backend_constraints: Constraints
    frontend_available: Constraints = DEFAULT_CONSTRAINTS             # resources currently available
    backend_available: Constraints = DEFAULT_CONSTRAINTS


class BasicAsset(BaseModel):
    uid: str = DEFAULT_UID
    schema_version: str = SCHEMA_VERSION
    timestamps: TimeStamps = DEFAULT_TIMESTAMP
    description: Optional[str] = Field(description='description')
    error: Optional[str] = Field(description="error description")
    terminate: Optional[bool] = Field(description="terminate")


class MlexJob(BasicAsset):
    service_type: ServiceType
    mlex_app: str = Field(description="MLExchange app associated with the job")
    job_kwargs: Union[DockerJob]
    working_directory: str = Field(description="dataset uri")
    status: Status = DEFAULT_STATUS
    pid: str = DEFAULT_JOB_PID
    requirements: Optional[Resources]
    logs: Optional[str]
    dependencies: List[str] = DEFAULT_UID_LIST
    class Config:
        extra = Extra.ignore


class MlexWorker(BasicAsset):
    service_type: ServiceType
    host_uid: str = Field(description='remote MLExchange host identifier')
    status: Status = DEFAULT_STATUS
    jobs_list: List[str] = DEFAULT_UID_LIST
    requirements: Optional[WorkerRequirements] = Field(description='computational requirements')
    dependencies: List[int] = []
    class Config:
        extra = Extra.ignore


class MlexWorkflow(BasicAsset):
    service_type: ServiceType = DEFAULT_SERVICE
    user_uid: str = Field(description='user identifier')
    workers_list: List[str] = DEFAULT_UID_LIST
    status: Status = DEFAULT_STATUS
    class Config:
        extra = Extra.ignore


class UserWorkflow(MlexWorkflow):
    job_list: List[MlexJob]
    dependencies: dict
    host_list: List[str] = Field(description='list of hostnames')
    requirements: Optional[CompRequirements] = Field(description='computational requirements')
