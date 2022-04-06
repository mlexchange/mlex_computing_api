from uuid import uuid4
from pymongo.mongo_client import MongoClient
from typing import List


from model import SimpleJob, DeployLocation


class JobNotFound(Exception):
    pass


class JobService:
    def __init__(self, client, db_name=None):
        """ The service creates a dataset called job_list
        """
        if db_name is None:
            db_name = 'job_list'
        self._db = client[db_name]
        self._collection_job_list = self._db.job_list
        self._create_indexes()

    def submit_job(self, simplejob: SimpleJob) -> SimpleJob:
        simplejob.uid = str(uuid4())
        simplejob_dict = simplejob.dict()
        self._collection_job_list.insert_one(simplejob_dict)
        self._clean_id(simplejob_dict)
        return simplejob

    def find_jobs(self,
                  user: str = None,
                  mlex_app: str = None,
                  job_type: str = None,
                  deploy_location: DeployLocation = None
                  ) -> List[SimpleJob]:
        """ Finds jobs that match the query parameters
        :param user: username
        :param mlex_app: source mlexchange app
        :param job_type: type of job
        :param deploy_location: deployment location

        :return: list of jobs
        """
        subqueries = []
        query = {}

        if user:
            subqueries.append({"user": user})

        if mlex_app:
            subqueries.append({"mlex_app": mlex_app})

        if job_type:
            subqueries.append({"job_type": job_type})

        if deploy_location:
            subqueries.append({"deploy_location": deploy_location})

        if len(subqueries) > 0:
            query = {"$and": subqueries}

        jobs = []
        for item in self._collection_job_list.find(query):
            self._clean_id(item)
            jobs.append(SimpleJob.parse_obj(item))

        return jobs

    def terminate_job(self, job_uid: str):
        """
        :param job_uid: uid of job to be updated
        :param req: information to update
        """
        job = self._collection_job_list.find_one({"uid": job_uid})
        if not job:
            raise JobNotFound(f"no job with id: {job_uid}")

        self._collection_job_list.update_one(
            {'uid': job_uid},
            {'$set': {'terminate': True}}
        )

    def get_logs(self, uid: str):
        job = self._collection_job_list.find_one({"uid": uid})
        return job['container_logs']

    def _create_indexes(self):
        self._collection_job_list.create_index([('uid', 1)], unique=True)
        self._collection_job_list.create_index([('user', 1)])
        self._collection_job_list.create_index([('mlex_app', 1)])
        self._collection_job_list.create_index([('job_type', 1)])
        self._collection_job_list.create_index([('deploy_location', 1)])

    @staticmethod
    def _clean_id(data):
        """ Removes mongo ID
        """
        if '_id' in data:
            del data['_id']


class Context:
    db: MongoClient = None
    job_svc: JobService = None


context = Context
