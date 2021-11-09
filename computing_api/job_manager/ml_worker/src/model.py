from enum import Enum

from pydantic import BaseModel, Extra, Field
from typing import Optional


SCHEMA_VERSION = "0.1"
DEFAULT_UID = "425f6781-e42b-23e2-a341-2431564214523"
DEFAULT_JOB_STATUS = "sent_queue"
DEFAULT_JOB_PID = str(0)


class DeployLocation(str, Enum):
    local = "local"
    vaughan = "vaughan"
    mlsandbox = "mlsandbox"


class JobStatus(str, Enum):
    sent_queue = "sent_queue"
    running = "running"
    complete = "completed"
    failed = "failed"
    terminated = "terminated"


class SimpleJob(BaseModel):
    uid: str = DEFAULT_UID
    schema_version: str = SCHEMA_VERSION
    user: str = Field(description="user identifier")
    mlex_app: str = Field(description="app associated with the job")
    job_type: str = Field(description="type of job")
    job_pid: str = DEFAULT_JOB_PID
    description: Optional[str] = Field(description="job description")
    status: JobStatus = DEFAULT_JOB_STATUS
    error: Optional[str] = Field(description="error description")
    deploy_location: DeployLocation
    gpu: bool = Field(description="run in gpu")
    terminate: Optional[bool] = Field(description="terminate process")
    data_uri: str = Field(description="dataset uri")
    container_uri: str = Field(description="container uri")
    container_cmd: str = Field(description="command to run")
    container_kwargs: Optional[dict] = Field(description="container kwargs") #Optional[str] = Field(description="container kwargs")
    container_logs: Optional[str] = Field(description="container logs")

    class Config:
        extra = Extra.forbid


class PatchRequest(BaseModel):
    status: JobStatus
    pid: Optional[str]

    container_cmd: str = Field(description="command to run")
    container_kwargs: Optional[str] = Field(description="container kwargs")

    class Config:
        extra = Extra.forbid


class PatchRequest(BaseModel):
    status: JobStatus
    pid: Optional[str]
