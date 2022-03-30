from uuid import uuid4
import math
from pymongo.mongo_client import MongoClient
from pymongo import ReturnDocument
from typing import List

from model import UserWorkflow, MlexWorkflow, MlexWorker, MlexJob, MlexHost, Status, WorkerRequirements


class JobNotFound(Exception):
    pass

class WorkerNotFound(Exception):
    pass

class WorkflowNotFound(Exception):
    pass


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
        if mlex_host.num_available_processors == 0:
            mlex_host.num_available_processors = mlex_host.max_processors
        if mlex_host.num_available_gpus == 0:
            mlex_host.num_available_gpus = mlex_host.max_gpus
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
        # if mlex_workflow.valid:
        mlex_jobs = workflow.job_list
        mlex_jobs_dict = []
        for job in mlex_jobs:
            job.uid = str(uuid4())
            mlex_jobs_dict.append(job.dict())
        mlex_workers_dict, workers_uids = self.split_workers(workflow)
        mlex_workflow.workers_list = workers_uids
        mlex_workflow_dict = mlex_workflow.dict()
        self._collection_workflow_list.insert_one(mlex_workflow_dict)
        self._collection_worker_list.insert_many(mlex_workers_dict)
        self._collection_job_list.insert_many(mlex_jobs_dict)
        return mlex_workflow.uid
        # return -1

    def get_host(self,
                 host_uid: str = None,
                 hostname: str = None,
                 nickname: str = None
                 ) -> MlexHost:
        '''
        Finds a host that matches the query parameters
        Args:
            host_uid
            hostname
            nickname
        Returns:
            MLExchange host with a description of its resources
        '''
        subqueries = []
        query = {}
        if host_uid:
            subqueries.append({"uid": host_uid})
        if hostname:
            subqueries.append({"hostname": hostname})
        if nickname:
            subqueries.append({"nickname": nickname})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        host = self._collection_resources_list.find_one(query)
        mlex_host = None
        if host:
            self._clean_id(host)
            mlex_host = MlexHost.parse_obj(host)
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
                      host_uid: str = None
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
                    status: Status = None
                    ) -> List[MlexWorker]:
        '''
        Finds workers that match the query parameters
        Args:
            host_uid:       host uid
            status:         status
        Returns:
            List of workers that match the query
        '''
        subqueries = []
        query = {}
        if host_uid:
            subqueries.append({"host_uid": host_uid})
        if status:
            subqueries.append({"status": status})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        workers = []
        for item in self._collection_worker_list.find(query):
            self._clean_id(item)
            workers.append(MlexWorker.parse_obj(item))
        return workers

    def get_next_worker(self,
                        host_uid: str
                        ) -> MlexWorker:
        '''
        Finds next worker in queue to be executed at host location and update the status of the worker and the resources
        at the host location
        Args:
            host_uid:       host uid
        Returns:
            Next worker to be executed
        '''
        mlex_host = self.get_host(host_uid=host_uid)
        worker = None
        if mlex_host:
            if mlex_host.num_running_workers < mlex_host.max_nodes:
                available_processors = mlex_host.num_available_processors
                available_gpus = mlex_host.num_available_gpus
                list_gpus = mlex_host.list_available_gpus
                # get next worker
                next_worker = self._collection_worker_list.find_one_and_update(
                    {
                        "host_uid": host_uid,
                        "status": "queue",
                        "requirements.num_processors":
                            {'$lte': available_processors},
                        "requirements.num_gpus":
                            {'$lte': available_gpus}
                    },
                    {"$set": {"status": "running"},
                     # "$push": {"requirements.list_gpus": {"$each": list_gpus, "$slice": "$$num_gpus"}}},
                     },
                    return_document = ReturnDocument.AFTER)     # returns the updated worker
                if next_worker:     # if a new worker can be launched, update the available resources status in host
                    self._clean_id(next_worker)
                    worker = MlexWorker.parse_obj(next_worker)
                    worker_gpus = worker.requirements.num_gpus
                    worker = self._collection_worker_list.find_one_and_update(
                        {"uid": worker.uid},
                        {"$set": {"requirements.list_gpus": list_gpus[0:worker_gpus]}},
                        return_document=ReturnDocument.AFTER
                    )
                    worker = MlexWorker.parse_obj(worker)
                    available_processors -= worker.requirements.num_processors
                    available_gpus -= worker.requirements.num_gpus
                    del list_gpus[0:worker_gpus]
                    running_workers = mlex_host.num_running_workers + 1
                    self._collection_resources_list.update_one({"uid": host_uid},
                                                               {"$set": {"num_available_processors": available_processors,
                                                                         "num_available_gpus": available_gpus,
                                                                         "list_available_gpus": list_gpus,
                                                                         "num_running_workers": running_workers}})
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
                uid: str
                ) -> MlexJob:
        '''
        Finds the job that matches the query parameters
        Args:
            uid:   job uid
        Returns:
            Job that matches the query
        '''
        item = self._collection_job_list.find_one_and_update({"uid": uid}, {"$set": {"status": "running"}})
        if not item:
            raise JobNotFound(f"no job with id: {uid}")
        self._clean_id(item)
        job = MlexJob.parse_obj(item)
        return job

    def get_jobs(self,
                 user: str = None,
                 mlex_app: str = None,
                 host_uid: str = None,
                 status: Status = None,
                 ) -> List[MlexJob]:
        '''
        Finds jobs that match the query parameters
        Args:
            user:       username
            mlex_app:   MLExchange app associated with job
            host_uid:   host uid
            status:     job status
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
        if status:
            query.append({"$match": {"status": status}})
        jobs = []
        for item in self._collection_job_list.aggregate(query):
            self._clean_id(item)
            jobs.append(MlexJob.parse_obj(item))
        return jobs

    def update_host(self, host_uid: str, worker_requirements: WorkerRequirements):
        '''
        Releases the computing resources back to the host
        Args:
            host_uid:               Host unique identifier
            worker_requirements:    Work requirements
        Returns:
            None
        '''
        num_processors = worker_requirements.num_processors
        num_gpus = worker_requirements.num_gpus
        list_gpus = worker_requirements.list_gpus
        self._collection_resources_list.update_one({"uid": host_uid},
                                                   {"$addToSet": {"list_available_gpus": {"$each": list_gpus}},
                                                    "$inc": {"num_available_processors": num_processors,
                                                             "num_available_gpus": num_gpus,
                                                             "num_running_workers": -1}
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
            self._collection_workflow_list.update_one(
                {'uid': workflow_uid},
                {'$set': {'status': status}}
            )
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

        self._collection_workflow_list.update_one(
            {'uid': workflow_uid},
            {'$set': {'terminate': True}}
        )
        for worker in workflow.workers_list:
            self.terminate_worker(worker_uid=worker)

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
        if worker.status != status:                     # update if status has changed
            self._collection_worker_list.update_one(
                {'uid': worker_uid},
                {'$set': {'status': status}}
            )
            if status in ['complete', 'complete with errors', 'failed', 'terminated']:
                self.update_host(worker.host_uid, worker.requirements)
            workflow = self.get_workflow(worker_uid=worker_uid)
            last_worker = True                          # check if this is the last job in worker
            for item_uid in workflow.workers_list:
                item = self.get_worker(uid=item_uid)
                if item.status in ['running', 'queue', 'warning'] and item.uid != worker_uid:
                    last_worker = False
                    break
            # check if it is the last worker in workflow with error/termination
            if last_worker and workflow.status == 'warning' and status in ['complete', 'complete with errors']:
                status = 'complete with errors'
            # if it is not the last worker in workflow and the worker has failed, completed with errors or was
            # terminated or canceled, the workflow is tagged with a "warning"
            elif workflow.status == 'warning' or status in ['failed', 'terminated', 'canceled', 'complete with errors']:
                status = 'warning'
            # if it is not the last worker, but it has completed it's execution
            elif not last_worker and status == 'complete':
                status = 'running'
            if workflow.status != status:               # update if status has changed
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

        self._collection_worker_list.update_one(
            {'uid': worker_uid},
            {'$set': {'terminate': True}}
        )

    def update_job(self, job_uid: str, status: Status):
        '''
        Update the status of a given job and the worker associated with this job
        Args:
            job_uid:    job unique identifier
            status:     job status
        Returns:
            None
        '''
        job = self.get_job(uid = job_uid)
        if job.status != status:                                    # update if status has changed
            self._collection_job_list.update_one(
                {'uid': job_uid},
                {'$set': {'status': status}}
            )
            worker = self.get_worker(job_uid=job_uid)              # retrieve worker information
            last_job = True                                         # check if this is the last job in worker
            for item_uid in worker.jobs_list:
                item = self.get_job(uid=item_uid)
                print(item.status)
                if item.status in ['running', 'queue'] and item.uid != job_uid:
                    last_job = False
                    break
            if status in ['failed', 'terminated', 'canceled']:      # if the job failed or was terminated/canceled,
                status = 'warning'                                  # the worker is tagged as "warning"

            # check if it is the last job in worker with error/termination
            if last_job and (worker.status == 'warning' or status == 'warning'):
                status = 'complete with errors'
            # if it is not the last job in worker and there was a previous error/termination
            elif worker.status == 'warning':
                status = 'warning'
            # if it is not the last job, but it has completed it's execution
            elif not last_job and status == 'complete':
                status = 'running'

            if worker.status != status:  # update if status has changed
                self.update_worker(worker_uid=worker.uid, status=status)
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

        self._collection_job_list.update_one(
            {'uid': job_uid},
            {'$set': {'terminate': True}}
        )
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
        requirements = user_workflow.requirements
        job_list = user_workflow.job_list
        constraints = requirements.constraints
        if constraints:
            num_nodes = 0
            list_num_processors = []
            list_num_gpus = []
            for constraint in constraints:
                num_nodes += constraint.num_nodes
                list_num_processors.extend([constraint.num_processors] * num_nodes)
                list_num_gpus.extend([constraint.num_gpus] * num_nodes)
        else:
            num_nodes = requirements.num_nodes
            num_processors = requirements.num_processors
            num_gpus = requirements.num_gpus
            host_uid = requirements.host_uid

        jobs_uid = [job.uid for job in user_workflow.job_list]
        num_jobs_per_node = math.ceil(len(job_list) / num_nodes)
        workers = []
        worker_uid_list = []
        for node in range(num_nodes):
            if constraints:
                num_processors = list_num_processors[node]
                num_gpus = list_num_gpus[node]

            host = self.get_host()
            host_uid = MlexHost.parse_obj(host).uid      # assigning first host for now! **warning**
            # host_uid = reserve_comp_resources(host_uid=host_uid, num_processors=num_processors, num_gpus=num_gpus)  # reserves comp resources
            jobs_in_node = jobs_uid[node * num_jobs_per_node:(node + 1) * num_jobs_per_node]
            worker = MlexWorker(uid=str(uuid4()),
                                host_uid=host_uid,
                                jobs_list=jobs_in_node,
                                requirements=WorkerRequirements.parse_obj(requirements))
            worker_uid_list.append(worker.uid)
            worker_dict = worker.dict()
            workers.append(worker_dict)
        return workers, worker_uid_list

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
        """ Removes mongo ID
        """
        if '_id' in data:
            del data['_id']


class Context:
    db: MongoClient = None
    comp_svc: ComputeService = None


context = Context
