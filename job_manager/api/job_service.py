from datetime import datetime
from itertools import compress
from uuid import uuid4
import math
import numpy as np
from ortools.sat.python import cp_model
import random
from pymongo.mongo_client import MongoClient
from pymongo import ReturnDocument
from typing import List

from model import UserWorkflow, MlexWorkflow, MlexWorker, MlexJob, MlexHost, Status, WorkerRequirements, ServiceType, \
                  Constraints, ResourcesQuery, States


class JobNotFound(Exception):
    pass

class WorkerNotFound(Exception):
    pass

class WorkflowNotFound(Exception):
    pass

class WorkflowNotValid(Exception):
    pass

MAX_COST = 1E10         # cost if not enough resources for cost

class ComputeService:
    def __init__(self, client, db_name=None):
        """
        The service creates the main database and it's collections
        """
        if db_name is None:
            db_name = 'job_manager'
        self._db = client[db_name]
        self._collection_resources_list = self._db.resources_list
        self._collection_workflow_list = self._db.workflow_list
        self._collection_worker_list = self._db.worker_list
        self._collection_job_list = self._db.job_list
        self._create_indexes()

    def submit_host(self, host: MlexHost) -> MlexHost:
        '''
        Submits host to MLExchange
        Args:
            host: [MlexHost] host to add
        Returns:
            host_uid
        '''
        host.uid = str(uuid4())
        mlex_host = MlexHost.parse_obj(host)
        mlex_host.frontend_available = mlex_host.frontend_constraints
        mlex_host.backend_available = mlex_host.backend_constraints
        mlex_host_dict = mlex_host.dict()
        self._collection_resources_list.insert_one(mlex_host_dict)
        return mlex_host.uid

    def submit_workflow(self, workflow: UserWorkflow) -> MlexWorkflow:
        '''
        Submits workflow to MLExchange and queues its respective workers to host
        Args:
            workflow: Workflow to execute
        Returns:
            workflow_uid or -1
        '''
        workflow.uid = str(uuid4())
        mlex_workflow = MlexWorkflow.parse_obj(workflow)
        mlex_workflow_dict, mlex_workers_dict, mlex_jobs_dict = self.split_workers(workflow)
        self._collection_workflow_list.insert_one(mlex_workflow_dict)
        self._collection_worker_list.insert_many(mlex_workers_dict)
        self._collection_job_list.insert_many(mlex_jobs_dict)
        return mlex_workflow.uid
        # return -1

    def get_host(self,
                 host_uid: str = None,
                 hostnames: List[str] = None,
                 nickname: str = None,
                 resources_query: ResourcesQuery = None
                 ) -> MlexHost:
        '''
        Finds a host that matches the query parameters
        Args:
            host_uid:       Host UID
            hostnames:      List of hostnames
            nickname:       Host nickname
            resources:      List of resources
        Returns:
            MLExchange host with a description of its resources
        '''
        subqueries = []
        query = {}
        if host_uid:
            subqueries.append({"uid": host_uid})
        if hostnames:
            subqueries.append({"hostname": {"$in": hostnames}})
        if nickname:
            subqueries.append({"nickname": nickname})
        if resources_query:
            num_processors = resources_query.num_processors
            num_gpus = resources_query.num_gpus
            service_type = resources_query.service_type
            if service_type == 'backend':
                subqueries.append({"backend_constraints.num_processors": {"$gte": num_processors}})
                subqueries.append({"backend_constraints.num_gpus": {"$gte": num_gpus}})
            if service_type == 'frontend':
                subqueries.append({"frontend_constraints.num_processors": {"$gte": num_processors}})
                subqueries.append({"frontend_constraints.num_gpus": {"$gte": num_gpus}})
            if service_type == 'hybrid':
                subqueries.append({"$expr": {"$gte": [{"$sum": ["$frontend_constraints.num_processors",
                                                                "$backend_constraints.num_processors"]},
                                                       num_processors]}})
                subqueries.append({"$expr": {"$gte": [{"$sum": ["$frontend_constraints.num_gpus",
                                                                "$backend_constraints.num_gpus"]},
                                                       num_gpus]}})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        host = self._collection_resources_list.find_one(query)
        mlex_host = None
        if host:
            self._clean_id(host)
            mlex_host = MlexHost.parse_obj(host)
        return mlex_host

    def get_hosts(self,
                 hostname: str = None,
                 nickname: str = None
                 ) -> List[MlexHost]:
        '''
        Finds a host that matches the query parameters
        Args:
            hostname:      Hostname
            nickname:      Nickname
        Returns:
            MLExchange host with a description of its resources
        '''
        subqueries = []
        query = {}
        if hostname:
            subqueries.append({"hostname": hostname})
        if nickname:
            subqueries.append({"nickname": nickname})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        hosts = self._collection_resources_list.find(query)
        mlex_host = []
        for host in hosts:
            self._clean_id(host)
            mlex_host.append(MlexHost.parse_obj(host))
        return mlex_host

    def get_workflow(self,
                     uid: str = None,
                     worker_uid: str = None
                     ) -> MlexWorkflow:
        '''
        Finds the workflow that matches the query parameters
        Args:
            uid:   workflow uid
            worker_uid:     worker uid
        Returns:
            Workflow that matches the query
        '''
        if uid:
            item = self._collection_workflow_list.find_one({"uid": uid})
            if not item:
                raise WorkflowNotFound(f"no workflow with id: {uid}")
        else:
            item = self._collection_workflow_list.find_one({"workers_list": worker_uid})
            if not item:
                raise WorkflowNotFound(f"no workflow with worker_uid: {worker_uid}")
        self._clean_id(item)
        workflow = MlexWorkflow.parse_obj(item)
        return workflow

    def get_workflows(self,
                      user: str = None,
                      host_uid: str = None,
                      state: States = None,
                      ) -> List[MlexWorkflow]:
        '''
        Finds list of workflows that match the query parameters
        Args:
            user:           username
            host_uid:       host uid
        Returns:
            List of workflows that match the query
        '''
        query = []
        if user:
            query.append({"$match": {"user_uid": user}})
        if host_uid:
            query += [{"$lookup": {"from": "worker_list",
                                   "let": {"workers_list": "$workers_list"},
                                   "pipeline": [{"$match": {"$expr": {"$in": ["$uid", "$$workers_list"]}}}],
                                   "as": "workers"}},
                      {"$match": {"workers.host_uid": host_uid}}]
        if state:
            query.append({"$match": {"status.state": state}})
        # if worker_uid:
        #     query.append({"$match": {"workers_list": worker_uid}})
        workflows = []
        for item in self._collection_workflow_list.aggregate(query):
            self._clean_id(item)
            workflows.append(MlexWorkflow.parse_obj(item))
        return workflows

    def get_worker(self,
                   uid: str = None,
                   job_uid: str = None
                   ) -> MlexWorker:
        '''
        Finds the worker that matches the query parameters
        Args:
            uid:   worker uid
            job_uid:        job uid
        Returns:
            Worker that matches the query
        '''
        if uid:
            item = self._collection_worker_list.find_one({"uid": uid})
            if not item:
                raise WorkerNotFound(f"no worker with id: {uid}")
        else:
            item = self._collection_worker_list.find_one({"jobs_list": job_uid})
            if not item:
                raise WorkerNotFound(f"no worker with job_uid: {job_uid}")
        self._clean_id(item)
        worker = MlexWorker.parse_obj(item)
        return worker

    def get_workers(self,
                    host_uid: str = None,
                    state: States = None
                    ) -> List[MlexWorker]:
        '''
        Finds workers that match the query parameters
        Args:
            host_uid:       host uid
            state:          state
        Returns:
            List of workers that match the query
        '''
        subqueries = []
        query = {}
        if host_uid:
            subqueries.append({"host_uid": host_uid})
        if state:
            subqueries.append({"status.state": state})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        workers = []
        for item in self._collection_worker_list.find(query):
            self._clean_id(item)
            workers.append(MlexWorker.parse_obj(item))
        return workers

    def get_next_worker(self,
                        host_uid: str,
                        service_type: ServiceType,
                        ) -> MlexWorker:
        '''
        Finds next worker in queue to be executed at host location and update the status of the worker and the resources
        at the host location
        Args:
            host_uid:       host uid
            service_type:   frontend, backend, or hybrid
        Returns:
            Next worker to be executed
        '''
        mlex_host = self.get_host(host_uid=host_uid)
        worker = None
        if mlex_host:
            if service_type == "frontend":
                available_processors = mlex_host.frontend_available.num_processors
                available_gpus = mlex_host.frontend_available.num_gpus
                list_gpus = mlex_host.frontend_available.list_gpus
                available_workers = mlex_host.frontend_available.num_nodes

            if service_type == "backend":
                available_processors = mlex_host.backend_available.num_processors
                available_gpus = mlex_host.backend_available.num_gpus
                list_gpus = mlex_host.backend_available.list_gpus
                available_workers = mlex_host.backend_available.num_nodes

            if service_type == "hybrid":
                f_num_processors = mlex_host.frontend_available.num_processors
                b_num_processors = mlex_host.backend_available.num_processors
                available_processors =  b_num_processors + f_num_processors

                f_num_gpus = mlex_host.frontend_available.num_gpus
                b_num_gpus = mlex_host.backend_available.num_gpus
                available_gpus = f_num_gpus + b_num_gpus

                f_gpus = mlex_host.frontend_available.list_gpus
                b_gpus = mlex_host.backend_available.list_gpus

                f_num_nodes = mlex_host.frontend_available.num_nodes
                b_num_nodes = mlex_host.backend_available.num_nodes
                available_workers = f_num_nodes + b_num_nodes
            if available_workers > 0:
                next_worker = self._collection_worker_list.find_one_and_update(
                    {
                        "host_uid": host_uid,
                        "service_type": service_type,
                        "status.state": "queue",
                        "requirements.num_processors": {'$lte': available_processors},
                        "requirements.num_gpus": {'$lte': available_gpus},
                        "dependencies": 0
                    },
                    {"$set": {"status.state": "running", "timestamps.execution_time": datetime.utcnow()},
                     # "$push": {"requirements.list_gpus": {"$each": list_gpus, "$slice": "$$num_gpus"}}},
                     },
                    return_document=ReturnDocument.AFTER)  # returns the updated worker

                if next_worker:  # if a new worker can be launched, update the available resources status in host
                    self._clean_id(next_worker)
                    worker = MlexWorker.parse_obj(next_worker)
                    num_processors = worker.requirements.num_processors
                    num_gpus = worker.requirements.num_gpus
                    if service_type == 'hybrid':
                        (f_av_num_gpus, b_av_num_gpus), (f_aloc_num_gpus, b_aloc_num_gpus) = \
                            self._update_hybrid_resources(f_num_gpus, b_num_gpus, num_gpus)
                        (f_av_num_processors, b_av_num_processors), (f_aloc_num_processors, b_aloc_num_processors) = \
                            self._update_hybrid_resources(f_num_processors, b_num_processors, num_processors)
                        (f_av_num_nodes, b_av_num_nodes), (f_aloc_num_nodes, b_aloc_num_nodes) = \
                            self._update_hybrid_resources(f_num_nodes, b_num_nodes, 1)
                        worker = self._collection_worker_list.find_one_and_update(
                            {"uid": worker.uid},
                            {"$set": {"requirements.list_gpus": f_gpus[0:f_aloc_num_gpus] + b_gpus[0:b_aloc_num_gpus],
                                      "requirements.kwargs": {
                                          "num_processors": f_aloc_num_processors,
                                          "num_gpus": f_aloc_num_gpus,
                                          "list_gpus": f_gpus[0:f_aloc_num_gpus],
                                          "num_nodes": f_aloc_num_nodes
                                      }
                             }},return_document = ReturnDocument.AFTER
                        )
                        del f_gpus[0:f_aloc_num_gpus]
                        del b_gpus[0:b_aloc_num_gpus]
                        self._collection_resources_list.update_one(
                            {"uid": host_uid},
                            {"$set": {
                                "frontend_available.num_processors": f_av_num_processors,
                                "frontend_available.num_gpus": f_av_num_gpus,
                                "frontend_available.list_gpus": f_gpus,
                                "frontend_available.num_nodes": f_av_num_nodes,
                                "backend_available.num_processors": b_av_num_processors,
                                "backend_available.num_gpus": b_av_num_gpus,
                                "backend_available.list_gpus": b_gpus,
                                "backend_available.num_nodes": b_av_num_nodes}})

                    else:
                        worker = self._collection_worker_list.find_one_and_update(
                            {"uid": worker.uid},
                            {"$set": {"requirements.list_gpus": list_gpus[0:num_gpus]}},
                            return_document=ReturnDocument.AFTER
                        )
                        available_processors -= num_processors
                        available_gpus -= num_gpus
                        del list_gpus[0:num_gpus]
                        available_workers -= 1
                        if service_type == "frontend":
                            self._collection_resources_list.update_one(
                                {"uid": host_uid},
                                {"$set": {
                                    "frontend_available.num_processors": available_processors,
                                    "frontend_available.num_gpus": available_gpus,
                                    "frontend_available.list_gpus": list_gpus,
                                    "frontend_available.num_nodes": available_workers}})
                        if service_type == "backend":
                            self._collection_resources_list.update_one(
                                {"uid": host_uid},
                                {"$set": {
                                    "backend_available.num_processors": available_processors,
                                    "backend_available.num_gpus": available_gpus,
                                    "backend_available.list_gpus": list_gpus,
                                    "backend_available.num_nodes": available_workers}})
                    self._clean_id(worker)
        return worker

    def get_job(self,
                uid: str
                ) -> MlexJob:
        '''
        Finds the job that matches the query parameters
        Args:
            uid:   job uid
        Returns:
            Job that matches the query
        '''
        item = self._collection_job_list.find_one({"uid": uid})
        if not item:
            raise JobNotFound(f"no job with id: {uid}")
        self._clean_id(item)
        job = MlexJob.parse_obj(item)
        return job

    def get_next_job(self,
                worker_uid: str
                ) -> MlexJob:
        '''
        Finds the job that matches the query parameters
        Args:
            worker_uid:   worker UID
        Returns:
            Job that matches the query
        '''
        status = Status(**{"state": "running"})
        worker = self.get_worker(uid=worker_uid)
        jobs_list = worker.jobs_list
        job = None
        self._collection_job_list.aggregate(
            [{"$match": {"uid": {"$in": jobs_list}, "status.state": "queue"}},
             {"$lookup": {"from":"job_list",
                          "let": {"dep": "$dependencies"},
                          "pipeline": [{"$match": {"$expr": {"$in": ["$uid", "$$dep"]}}},
                                       {"$group": {"_id": 0, "state":{"$push":"$status.state"}}}],
                          "as": "dependencies_status"}},
             {"$set": {"dependencies_status": {"$cond": {"if": {"$eq": ["$dependencies_status", []]},
                                                             "then": [["no_dependencies"]],
                                                             "else": "$dependencies_status.state"}}
                           }},
             {"$unwind": "$dependencies_status"},
             {"$set": {"status.state": {"$cond": {"if": {"$in": ["queue", "$dependencies_status"]},
                                                  "then": "$status.state",
                                                  "else": {
                                                      "$cond": {"if": {"$in": ["running", "$dependencies_status"]},
                                                                "then": "$status.state",
                                                                "else": "running"}
                                                  }}
                                        }}},
             {"$project": {"dependencies_status": 0}},
             {"$merge": {"into": "job_list",
                         "whenMatched": "replace",
                         "whenNotMatched": "discard"}}
             ])
        item = self._collection_job_list.find_one_and_update({"uid": {"$in": jobs_list},
                                                              "status.state": "running",
                                                              "timestamps.execution_time": {"$type":"null"}},
                                                             {"$set": {"timestamps.execution_time": datetime.utcnow()}})
        if item:
            self._clean_id(item)
            job = MlexJob.parse_obj(item)
        return job

    def get_jobs(self,
                 user: str = None,
                 mlex_app: str = None,
                 host_uid: str = None,
                 state: States = None,
                 ) -> List[MlexJob]:
        '''
        Finds jobs that match the query parameters
        Args:
            user:       username
            mlex_app:   MLExchange app associated with job
            host_uid:   host uid
            state:      job state
        Returns:
            List of jobs that match the query
        '''

        query = []
        if mlex_app:
            query.append({"$match": {"mlex_app": mlex_app}})
        if user:
            query += [{"$lookup": {"from": "worker_list",
                                   "let": {"job_uid": "$uid"},
                                   "pipeline": [{"$match": {"$expr": {"$in": ["$$job_uid", "$jobs_list"]}}}],
                                   "as": "workers"}},
                      {"$unwind": "$workers"},
                      {"$lookup": {"from": "workflow_list",
                                   "let": {"worker_uid": "$workers.uid"},
                                   "pipeline": [{"$match": {"$expr": {"$in": ["$$worker_uid", "$workers_list"]}}}],
                                   "as": "workflows"}},
                      {"$unwind": "$workflows"},
                      {"$match": {"workflows.user_uid": user}}]
        if host_uid:
            query += [{"$lookup": {"from": "worker_list",
                                   "let": {"job_uid": "$uid"},
                                   "pipeline": [{"$match": {"$expr": {"$in": ["$$job_uid", "$jobs_list"]}}}],
                                   "as": "workers"}},
                      {"$unwind": "$workers"},
                      {"$match": {"workers.host_uid": host_uid}}]
        if state:
            query.append({"$match": {"status.state": state}})
        jobs = []
        for item in self._collection_job_list.aggregate(query):
            self._clean_id(item)
            jobs.append(MlexJob.parse_obj(item))
        return jobs

    def update_host(self, host_uid: str, worker_requirements: WorkerRequirements, service_type: ServiceType):
        '''
        Releases the computing resources back to the host
        Args:
            host_uid:               Host unique identifier
            worker_requirements:    Work requirements
            service_type:           Backend, Frontend, Hybrid
        Returns:
            None
        '''
        num_processors = worker_requirements.num_processors
        num_gpus = worker_requirements.num_gpus
        list_gpus = worker_requirements.list_gpus
        if service_type == 'frontend':
            self._collection_resources_list.update_one(
                {"uid": host_uid},
                {"$addToSet": {"frontend_available.list_gpus": {"$each": list_gpus}},
                 "$inc": {"frontend_available.num_processors": num_processors,
                          "frontend_available.num_gpus": num_gpus,
                          "frontend_available.num_nodes": 1}
                 })
        if service_type == 'backend':
            self._collection_resources_list.update_one(
                {"uid": host_uid},
                {"$addToSet": {"backend_available.list_gpus": {"$each": list_gpus}},
                 "$inc": {"backend_available.num_processors": num_processors,
                          "backend_available.num_gpus": num_gpus,
                          "backend_available.num_nodes": 1}
                 })
        if service_type == 'hybrid':
            frontend_specs = Constraints.parse_obj(worker_requirements.kwargs)
            f_num_processors = frontend_specs.num_processors
            f_num_gpus = frontend_specs.num_gpus
            f_list_gpus = frontend_specs.list_gpus
            f_num_workers = frontend_specs.num_nodes
            self._collection_resources_list.update_one(
                {"uid": host_uid},
                {"$addToSet": {"frontend_available.list_gpus": {"$each": f_list_gpus},
                               "backend_available.list_gpus": {"$each": list(set(list_gpus)^set(f_list_gpus))}},
                 "$inc": {"frontend_available.num_processors": f_num_processors,
                          "frontend_available.num_gpus": f_num_gpus,
                          "frontend_available.num_nodes": f_num_workers,
                          "backend_available.num_processors": num_processors - f_num_processors,
                          "backend_available.num_gpus": num_gpus - f_num_gpus,
                          "backend_available.num_nodes": 1 - f_num_workers}
                 })
        pass

    def update_workflow(self, workflow_uid: str, status: Status):
        '''
        Update the status of a given workflow
        Args:
            workflow_uid:   workflow unique identifier
            status:         workflow status
        Returns:
            workflow uid
        '''
        workflow = self.get_workflow(uid=workflow_uid)
        if workflow.status != status:                     # update if status has changed
            if status.state == 'running':
                self._collection_workflow_list.update_one(
                    {'uid': workflow_uid},
                    {'$set': {'status': status.dict(), "timestamps.execution_time": datetime.utcnow()}})
            if status.state in ['complete', 'complete with errors', 'failed', 'terminated', 'canceled']:
                self._collection_workflow_list.update_one(
                    {'uid': workflow_uid},
                    {'$set': {'status': status.dict(), "timestamps.end_time": datetime.utcnow()}})
            else:
                self._collection_workflow_list.update_one(
                    {'uid': workflow_uid},
                    {'$set': {'status': status.dict()}})
        return workflow_uid

    def terminate_workflow(self, workflow_uid: str):
        '''
        Terminates a given workflow
        Args:
            workflow_uid: workflow unique identifier
        Returns:
            workflow uid
        '''
        workflow = self.get_workflow(uid=workflow_uid)
        if not workflow:
            raise JobNotFound(f"no workflow with id: {workflow_uid}")
        # terminate if if has not finalized yet
        self._collection_workflow_list.update_one(
            {'uid': workflow_uid, 'status.state': {'$nin': ['complete', 'failed', 'complete with errors']}},
            {'$set': {'terminate': True}}
        )
        self._collection_workflow_list.update_one(          # if the workflow is in queue, mark as canceled
            {'uid': workflow_uid, 'status.state': 'queue'},
            {'$set': {'terminate': True, 'status.state': 'canceled'}}
        )
        for worker in workflow.workers_list:
            self.terminate_worker(worker_uid=worker)
        pass

    def update_worker(self, worker_uid: str, status: Status):
        '''
        Updates the status of a given worker and it's associated workflow
        Args:
            worker_uid: worker unique identifier
            status:     worker status
        Returns:
            None
        '''
        worker = self.get_worker(uid=worker_uid)
        if worker.status.state != status.state:                     # update if status has changed
            if status.state in ['complete', 'complete with errors', 'failed', 'terminated', 'canceled']:
                self._collection_worker_list.update_one(
                    {'uid': worker_uid},
                    {'$set': {'status': status.dict(), "timestamps.end_time": datetime.utcnow()}})
                self.update_host(worker.host_uid, worker.requirements, worker.service_type)
            else:
                self._collection_worker_list.update_one(
                    {'uid': worker_uid},
                    {'$set': {'status': status.dict()}})
            workflow = self.get_workflow(worker_uid=worker_uid)
            last_worker = True                          # check if this is the last job in worker
            for item_uid in workflow.workers_list:
                item = self.get_worker(uid=item_uid)
                if item.status.state in ['running', 'queue', 'warning'] and item.uid != worker_uid:
                    last_worker = False
                    break
            # check if it is the last worker in workflow with error/termination
            if last_worker and workflow.status.state == 'warning' and \
                    status.state in ['complete', 'complete with errors']:
                status.state = 'complete with errors'
            # if it is not the last worker in workflow and the worker has failed, completed with errors or was
            # terminated or canceled, the workflow is tagged with a "warning"
            elif workflow.status.state == 'warning' or \
                    status.state in ['failed', 'terminated', 'canceled', 'complete with errors']:
                status.state = 'warning'
            # if it is not the last worker, but it has completed it's execution
            elif not last_worker and status.state == 'complete':
                status.state = 'running'
            if workflow.status.state != status.state:               # update if status has changed
                self.update_workflow(workflow_uid=workflow.uid, status=status)
        pass

    def terminate_worker(self, worker_uid: str):
        '''
        Terminates a given worker
        Args:
            worker_uid: worker unique identifier
        Returns:
            worker uid
        '''
        worker = self.get_worker(uid=worker_uid)
        if not worker:
            raise WorkerNotFound(f"no worker with id: {worker_uid}")
        # terminate if it has not been completed yet
        self._collection_worker_list.update_one(
            {'uid': worker_uid, 'status.state': {'$nin': ['complete', 'failed', 'complete with errors']}},
            {'$set': {'terminate': True}}
        )
        self._collection_worker_list.update_one(        # if the worker is in queue, mark as canceled
            {'uid': worker_uid, 'status.state': 'queue'},
            {'$set': {'terminate': True, 'status.state': 'canceled'}}
        )
        for job in worker.jobs_list:        # terminate the jobs in worker
            self.terminate_job(job)
        pass

    def update_job(self, job_uid: str, status: Status, logs: str = None):
        '''
        Update the status of a given job and the worker associated with this job
        Args:
            job_uid:    job unique identifier
            status:     job status
            logs:       job logs
        Returns:
            None
        '''
        job = self.get_job(uid = job_uid)
        if status:
            if job.status.state != status.state:                              # update if state has changed
                if status.state in ['complete', 'failed', 'terminated', 'canceled']:
                    self._collection_job_list.update_one(
                        {'uid': job_uid},
                        {'$set': {'status': status.dict(), "timestamps.end_time": datetime.utcnow()}})
                    self._update_dependencies(job_uid)
                else:
                    self._collection_job_list.update_one(
                        {'uid': job_uid},
                        {'$set': {'status': status.dict()}})
                worker = self.get_worker(job_uid=job_uid)               # retrieve worker information
                last_job = True                                         # check if this is the last job in worker
                for item_uid in worker.jobs_list:
                    item = self.get_job(uid=item_uid)
                    if item.status.state in ['running', 'queue'] and item.uid != job_uid:
                        last_job = False
                        break
                if status.state in ['failed', 'terminated', 'canceled']:      # if the job failed or was terminated/canceled,
                    status.state = 'warning'                                  # the worker is tagged as "warning"
                # check if it is the last job in worker with error/termination
                if last_job and (worker.status.state == 'warning' or status.state == 'warning'):
                    status.state = 'complete with errors'
                # if it is not the last job in worker and there was a previous error/termination
                elif worker.status.state == 'warning':
                    status.state = 'warning'
                # if it is not the last job, but it has completed it's execution
                elif not last_job and status.state == 'complete':
                    status.state = 'running'
                if worker.status.state != status.state:                       # update if state has changed
                    self.update_worker(worker_uid=worker.uid, status=status)
        elif logs:
            self._collection_job_list.update_one(
                {'uid': job_uid},
                {'$set': {'logs': logs}}
            )
        pass

    def terminate_job(self, job_uid: str):
        '''
        Terminates a given job
        Args:
            job_uid: job unique identifier
        Returns:
            None
        '''
        job = self._collection_job_list.find_one({"uid": job_uid})
        if not job:
            raise JobNotFound(f"no job with id: {job_uid}")
        # terminate job if it has not finalized yet
        self._collection_job_list.update_one(
            {'uid': job_uid, 'status.state': {'$nin': ['complete', 'failed']}},
            {'$set': {'terminate': True}}
        )
        results = self._collection_job_list.update_one(               # if the job is in queue, mark as canceled
            {'uid': job_uid, 'status.state': 'queue'},
            {'$set': {'terminate': True, 'status.state': 'canceled'}}
        )
        if results.modified_count>0:        # if the job is cancelled, update dependencies
            self._update_dependencies(job_uid)
        pass

    def split_workers(self, user_workflow: UserWorkflow):
        '''
        This function receives an user-defined MLExchange workflow and returns the lists of workers with their
        respectively assigned jobs
        Args:
            user_workflow:  User-defined workflow
        Returns:
            mlex_workers:   List[MlexWorker]
            mlex_jobs:      List[MLexJob]
        '''
        host_list = user_workflow.host_list
        requirements = user_workflow.requirements
        job_list = user_workflow.job_list
        constraints = requirements.constraints
        cost_matrix = []
        if constraints:
            num_nodes = 0
            list_num_processors = []
            list_num_gpus = []
            costs = []
            for constraint in constraints:
                num_nodes += constraint.num_nodes
                list_num_processors.extend([constraint.num_processors] * num_nodes)
                list_num_gpus.extend([constraint.num_gpus] * num_nodes)
                costs.extend(self._calculate_cost(job_list, constraint.num_processors, constraint.num_gpus) *
                             constraint.num_nodes)
        else:
            num_nodes = requirements.num_nodes
            num_processors = requirements.num_processors
            num_gpus = requirements.num_gpus
            host_uid = requirements.host_uid
            costs = (self._calculate_cost(job_list, num_processors, num_gpus) * num_nodes)
        cost_matrix = np.array(costs)
        cost_matrix = cost_matrix.reshape(num_nodes, len(job_list))
        print(f'Cost matrix: {cost_matrix}')
        if MAX_COST*num_nodes in sum(cost_matrix):
            raise WorkflowNotValid(f"the list of jobs cannot be completed with the arranged resources")
        allocation_matrix = self._assign_jobs(cost_matrix)
        mlex_jobs_dict = []
        jobs_uid = []
        services_type = []
        worker_dependencies = [0]*len(job_list)
        for ind, job in enumerate(job_list):
            job.uid = str(uuid4())
            jobs_uid.append(job.uid)                    # job uid
            job.dependencies = list(map(jobs_uid.__getitem__, user_workflow.dependencies[str(ind)]))
            worker_dependencies[ind] = len(job.dependencies)
            services_type.append(job.service_type)      # frontend vs backend
            mlex_jobs_dict.append(job.dict())
        mlex_workers_dict = []
        worker_uid_list = []
        for node in range(num_nodes):
            if constraints:
                num_processors = list_num_processors[node]
                num_gpus = list_num_gpus[node]
                requirements = {'num_processors':num_processors, "num_gpus": num_gpus}
            jobs_in_node = list(compress(jobs_uid, allocation_matrix[node,]))
            dependencies = list(compress(worker_dependencies, allocation_matrix[node,]))
            services_in_node = list(compress(services_type, allocation_matrix[node,]))
            if len(set(services_in_node))==1:
                service_type = services_in_node[0]      # if all are frontend or backend
            else:
                service_type = 'hybrid'
            resources_query = ResourcesQuery(num_processors=num_processors,
                                             num_gpus=num_gpus,
                                             service_type=service_type)
            host = self.get_host(hostnames=host_list, resources_query=resources_query)
            if not host:
                raise WorkflowNotValid(f"not enough resources to execute workflow")
            host_uid = MlexHost.parse_obj(host).uid
            worker = MlexWorker(uid=str(uuid4()),
                                service_type=service_type,
                                host_uid=host_uid,
                                jobs_list=jobs_in_node,
                                dependencies=dependencies,
                                requirements=WorkerRequirements.parse_obj(requirements))
            worker_uid_list.append(worker.uid)
            worker_dict = worker.dict()
            mlex_workers_dict.append(worker_dict)
        if len(set(services_type)) == 1:
            workflow_service_type = services_type[0]    # if all are frontend or backend
        else:
            workflow_service_type = 'hybrid'
        user_workflow.workers_list = worker_uid_list
        user_workflow.service_type = workflow_service_type
        return user_workflow.dict(), mlex_workers_dict, mlex_jobs_dict

    def _update_dependencies(self, job_uid):
        '''
        Updates the job dependencies accross workers
        Args:
            job_uid:    Job unique identifier
        Returns:
            None
        '''
        # update dependencies
        dependent_jobs = self._collection_job_list.find({'dependencies': job_uid})
        dependent_jobs = [dependent_job['uid'] for dependent_job in dependent_jobs]
        # update workers dependencies
        workers = self._collection_worker_list.find({'jobs_list': {'$in': dependent_jobs}})
        for worker in workers:
            worker = MlexWorker.parse_obj(worker)
            worker_jobs = worker.jobs_list
            for indx, worker_job in enumerate(worker_jobs):
                if worker_job in dependent_jobs:
                    self._collection_worker_list.update_one(
                        {"uid": worker.uid},
                        {"$inc": {f"dependencies.{indx}": -1}},
                    )
        # self._collection_worker_list.update_many(
        #     {'jobs_list': {'$in': dependent_jobs}},
        #     {'$inc': {"dependencies.$[ind]": -1}},
        #     {"arrayFilters": [{"jobs_list.$[ind]": {'$in': dependent_jobs} }]}
        # )
        #                              {"$cond": {"if": {"$in": ["dependencies.$", dependent_jobs]},
        #                         "then": -1,
        #                         "else": 0}}}
        #      }
        # )
        # #                     {"dependencies.$": -1}}

    def _create_indexes(self):
        self._collection_workflow_list.create_index([('uid', 1)], unique=True)
        self._collection_workflow_list.create_index([('user_uid', 1)])
        self._collection_workflow_list.create_index([('workflow_type', 1)])
        self._collection_workflow_list.create_index([('status', 1)])

        self._collection_worker_list.create_index([('uid', 1)], unique=True)
        self._collection_worker_list.create_index([('host_uid', 1)])
        self._collection_worker_list.create_index([('status', 1)])

        self._collection_job_list.create_index([('uid', 1)], unique=True)
        self._collection_job_list.create_index([('mlex_app', 1)])
        self._collection_job_list.create_index([('type', 1)])
        self._collection_job_list.create_index([('status', 1)])
        self._collection_job_list.create_index([('pid', 1)])

    @staticmethod
    def _clean_id(data):
        """
        Removes the mongo ID
        """
        if '_id' in data:
            del data['_id']

    @staticmethod
    def _calculate_cost(job_list, num_processors, num_gpus):
        '''
        Calculates the costs of the list of jobs for a given worker with the input constraints
        Args:
            job_list:           List of jobs
            num_processors:     Number of CPUs
            num_gpus:           Number of GPUs
        '''
        penalty = 10            # penalty for GPUs
        costs = []
        for job in job_list:
            cost = 0
            if job.requirements:
                job_num_processors = job.requirements.num_processors
                job_num_gpus = job.requirements.num_gpus
                if not job_num_processors:
                    job_num_processors = num_processors
                if not job_num_gpus:
                    job_num_gpus = num_gpus
                if num_processors>=job_num_processors and num_gpus>=job_num_gpus:
                    cost = num_processors - job_num_processors + penalty*(num_gpus - job_num_gpus)
                else:
                    cost = MAX_COST
            costs.append(cost)
        return costs

    @staticmethod
    def _assign_jobs(cost_matrix):
        '''
        Creates an allocation matrix for jobs and workers
        Args:
            cost_matrix:    Cost matrix
        '''
        num_workers, num_tasks = cost_matrix.shape
        demands = np.ones((num_tasks)).astype(int)
        supply = np.ones((num_workers)).astype(int)*num_tasks
        model = cp_model.CpModel()
        x = {}
        for worker in range(num_workers):
            for task in range(num_tasks):
                x[worker, task] = model.NewBoolVar(f'x[{worker}, {task}]')

        for worker in range(num_workers):
            model.Add(sum(x[worker, task] for task in range(num_tasks)) <= supply[worker])

        for worker in range(num_workers):
            model.Add(sum(x[worker, task] for task in range(num_tasks)) >= 1)

        for task in range(num_tasks):
            model.Add(sum(x[worker, task] for worker in range(num_workers)) == demands[task])

        objective_terms = []
        for worker in range(num_workers):
            for task in range(num_tasks):
                objective_terms.append(cost_matrix[worker][task] * x[worker, task])

        model.Minimize(sum(objective_terms))
        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            print(f'Total cost = {solver.ObjectiveValue()}\n')

        val = []
        for worker in range(num_workers):
            for task in range(num_tasks):
                try:
                    val.append(solver.Value(x[worker, task]))
                except:
                    print("error could not find value")
        
        sol = np.array(val)
        sol = sol.reshape(num_workers, num_tasks)
        print(f'Job assignment: {sol}')
        return sol

    @staticmethod
    def _update_hybrid_resources(front, back, job):
        '''
        Distributes the job resources to both frontend and backend sources
        Args:
            front:              Total number of frontend resources
            back:               Total number of backend resources
            job:                Number of resources requested by the job
        Returns:
            front_assigned:     Number of frontend resources allocated for the job
            back_assigned:      Number of backend resources allocated for the job
        '''
        if job>0:
            min_val = max((job-back)/job, 0)*job
            max_val = min(front/job, 1)*job
            front_assigned = random.randint(min_val, max_val)
            back_assigned = job - front_assigned
        else:
            front_assigned = 0
            back_assigned = 0
        return (front - front_assigned, back - back_assigned), (front_assigned, back_assigned)


class Context:
    db: MongoClient = None
    comp_svc: ComputeService = None


context = Context
