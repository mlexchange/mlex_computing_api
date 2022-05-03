import os, sys
sys.path.insert(0, os.path.abspath(".."))
import requests
from model import MlexWorker, MlexWorkflow, MlexHost, MlexJob, Status


COMP_URL = 'http://localhost:8080/api/v0/'


def test_submit_host(host):
    '''
    This test submits a new host to the database
    Args:
        rest_client: test client
    Returns:
        None
    '''
    response = requests.post(f'{COMP_URL}hosts', json=host)
    print(f'The host was submitted: {response.status_code == 200}')      # host was successfully submitted


def test_submit_workflow(workflow):
    '''
    This test submits a workflow and checks that the amount of workers and jobs matches it's initial description
    Args:
        requests:    test client
    Returns:
        None
    '''
    ## submit workflow
    response = requests.post(f'{COMP_URL}workflows', json=workflow)
    workflow_uid = response.json()
    print(f'The workflow was submitted: {response.status_code == 200}')      # workflow was successfully submitted

    ## retrieve workflow information
    mlex_workflow = requests.get(f'{COMP_URL}workflows/{workflow_uid}').json()
    mlex_workflow = MlexWorkflow.parse_obj(mlex_workflow)

    ## retrieve workers uids in workflow
    mlex_workers = []
    for worker_uid in mlex_workflow.workers_list:
        worker = requests.get(f'{COMP_URL}workers/{worker_uid}').json()
        mlex_workers.append(worker)

    ## retrieve jobs uids in workflow
    mlex_jobs = []
    for worker in mlex_workers:
        worker = MlexWorker.parse_obj(worker)
        for job_uid in worker.jobs_list:
            job = requests.get(COMP_URL+'jobs/'+job_uid).json()
            mlex_jobs.append(job)
    print(f'There are 4 jobs in this workflow: {len(mlex_jobs) == 4}')              # there are 4 jobs in this workflow


def test_get_next_workers():
    '''
    This test retrieves the next worker at certain host and checks if:
        - The worker status has been updated
        - The resources assigned to the worker has been updated at the host
    Args:
        requests: Test client
    Returns:
        None
    '''
    # get initial values for comparison
    host = requests.get(f'{COMP_URL}hosts', params={'nickname': 'test'}).json()
    mlex_host = MlexHost.parse_obj(host[0])
    init_frontend_available = mlex_host.frontend_available
    init_backend_available = mlex_host.backend_available

    # get next backend worker
    # this worker cannot be executed due to it's dependencies
    response = requests.get(f'{COMP_URL}private/workers', params={'host_uid': mlex_host.uid,
                                                                     'service_type': 'backend'}).json()
    print(f'The backedn worker cannot be executed due to dependencies: {response is None}')

    # get next hybrid worker
    response = requests.get(f'{COMP_URL}private/workers', params={'host_uid': mlex_host.uid,
                                                                     'service_type': 'hybrid'}).json()
    worker = MlexWorker.parse_obj(response)
    num_processors = worker.requirements.num_processors
    num_gpus = worker.requirements.num_gpus
    assigned_gpus = worker.requirements.list_gpus

    # get final values for comparison
    host = requests.get(f'{COMP_URL}hosts', params={'nickname': 'test'}).json()
    mlex_host = MlexHost.parse_obj(host[0])
    final_frontend_available = mlex_host.frontend_available
    final_backend_available = mlex_host.backend_available

    if worker.service_type == 'frontend':
        print('The following 5 statements check the requirements in the host')
        print(final_frontend_available.num_processors == init_frontend_available.num_processors - num_processors)
        print(final_frontend_available.num_gpus == init_frontend_available.num_gpus - num_gpus)
        print(init_frontend_available.list_gpus == assigned_gpus + final_frontend_available.list_gpus)
        print(init_frontend_available.num_nodes == final_frontend_available.num_nodes + 1)

        print(final_backend_available == init_backend_available)

    if worker.service_type == 'backend':
        print('The following 5 statements check the requirements in the host')
        print(final_backend_available.num_processors == init_backend_available.num_processors - num_processors)
        print(final_backend_available.num_gpus == init_backend_available.num_gpus - num_gpus)
        print(init_backend_available.list_gpus == assigned_gpus + final_backend_available.list_gpus)
        print(init_backend_available.num_nodes == final_backend_available.num_nodes + 1)

        print(final_frontend_available == init_frontend_available)

    if worker.service_type == 'hybrid':
        print('The following 4 statements check the requirements in the host')
        print(final_frontend_available.num_processors + final_backend_available.num_processors == \
               init_frontend_available.num_processors + init_backend_available.num_processors - num_processors)
        print(final_frontend_available.num_gpus + final_backend_available.num_gpus == \
               init_frontend_available.num_gpus + init_backend_available.num_gpus - num_gpus)
        print(list(set(init_frontend_available.list_gpus + init_backend_available.list_gpus) ^ \
               set(final_frontend_available.list_gpus + final_backend_available.list_gpus)) == assigned_gpus)
        print(init_frontend_available.num_nodes + init_backend_available.num_nodes ==\
               final_frontend_available.num_nodes + final_backend_available.num_nodes + 1)

    print(f'The worker is running in the host: {worker.host_uid == mlex_host.uid and worker.status.state == "running"}')


def test_dependencies_status():
    # get list of dependencies in queued worker
    status = {'state': 'queue'}
    workers = requests.get(f'{COMP_URL}workers', params=status).json()
    q_worker = workers[0]
    init_dependencies = q_worker['dependencies']
    # get list of jobs in running worker and termine it's job
    status = {'state': 'running'}
    workers = requests.get(f'{COMP_URL}workers', params=status).json()
    r_worker = workers[0]
    jobs = r_worker['jobs_list']
    response = requests.patch(f'{COMP_URL}jobs/{jobs[0]}/terminate')
    print(f'The job was terminated: {response.status_code == 200}')
    # check the other worker dependencies
    worker = requests.get(f'{COMP_URL}workers/{q_worker["uid"]}').json()
    final_dependencies = worker['dependencies']
    print(f'The dependencies are updating correctly: {final_dependencies[0] == init_dependencies[0] - 1}')


def test_update_status():
    # get next worker
    host = requests.get(f'{COMP_URL}hosts', params={'nickname': 'test'}).json()
    mlex_host = MlexHost.parse_obj(host[0])
    response = requests.get(f'{COMP_URL}private/workers', params={'host_uid': mlex_host.uid,
                                                                     'service_type': 'backend'}).json()
    worker = MlexWorker.parse_obj(response)
    # get next job
    response = requests.get(f'{COMP_URL}private/jobs/', params={'worker_uid': worker.uid})
    print(f'Get next job: {response.status_code == 200}')
    job = response.json()
    mlex_job = MlexJob.parse_obj(job)
    # change the status
    response = requests.patch(f'{COMP_URL}private/jobs/{mlex_job.uid}/update',
                               params={'logs': 'this is a test'})
    print(f'Change job status: {response.status_code == 200}')
    # check that the status has been changed correctly
    job = requests.get(f'{COMP_URL}jobs/{mlex_job.uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    print(f'The status changed correctly: {mlex_job.status.state == "running" and mlex_job.logs == "this is a test"}')
    # check that the status has changed too
    item = requests.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    print(f'Status of worker changed: {mlex_item.status.state == "running"}')

    # let's change the status of the only job as failed
    job_uid = worker.jobs_list[0]
    status = Status(**{'state': 'failed', 'return_code': 'Error 1234'})
    response = requests.patch(f'{COMP_URL}private/jobs/{job_uid}/update', json=status.dict())
    print(f'Change job status to failed: {response.status_code == 200}')
    # check that the status has been changed correctly
    job = requests.get(f'{COMP_URL}jobs/{job_uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    print(f'Check the job status: {mlex_job.status.state == "failed" and mlex_job.status.return_code == "Error 1234"}')
    # check that the worker status has changed too
    item = requests.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    print(f'Check the worker status: {mlex_item.status.state == "complete with errors"}')

    # check the status of the workflow
    workflow = requests.get(f'{COMP_URL}workflows').json()
    print(f'Check the # of workflows: {len(workflow) == 1}')
    mlex_workflow = MlexWorkflow.parse_obj(workflow[0])
    print(f'Check the workflow status: {mlex_workflow.status.state == "warning"}')


def test_terminate_processes():
    status = {'state': 'queue'}
    jobs = requests.get(f'{COMP_URL}jobs', params=status).json()
    mlex_jobs = []
    for job in jobs:
        mlex_jobs.append(MlexJob.parse_obj(job))
    print(f'Check there are 2 jobs left: {len(mlex_jobs) == 2}')       # the jobs in this worker have not been executed yet

    # terminate the first job
    first_job_uid = mlex_jobs[0].uid
    response = requests.patch(f'{COMP_URL}jobs/{first_job_uid}/terminate')
    print(f'Check one job was terminated: {response.status_code == 200}')
    job = requests.get(f'{COMP_URL}jobs/{first_job_uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    print(f'Terminated? {mlex_job.terminate}')

    # terminate the worker
    status = {'state': 'running'}
    worker = requests.get(f'{COMP_URL}workers', params=status).json()
    print(f'Check there is 1 job left: {len(worker) == 1}')  # worker 2 is still running
    worker = MlexWorker.parse_obj(worker[-1])
    response = requests.patch(f'{COMP_URL}workers/{worker.uid}/terminate')
    print(f'Check one worker was terminated: {response.status_code == 200}')
    item = requests.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    print(f'Terminated? {mlex_item.terminate}')

    # terminate workflow
    workflow = requests.get(f'{COMP_URL}workflows').json()
    print(f'Check the # of workflows: {len(workflow) == 1}')
    mlex_workflow = MlexWorkflow.parse_obj(workflow[0])
    response = requests.patch(f'{COMP_URL}workflows/{mlex_workflow.uid}/terminate')
    print(f'Terminate workflow: {response.status_code == 200}')
    item = requests.get(f'{COMP_URL}workflows/{mlex_workflow.uid}').json()
    mlex_item = MlexWorkflow.parse_obj(item)
    print(f'Terminated? {mlex_item.terminate}')

#################################################### TEST ELEMENTS ####################################################
host1 = {
    'nickname': 'test',
    'hostname': 'test.als.lbl.gov',
    'frontend_constraints': {'num_processors': 10,
                             'num_gpus': 2,
                             'list_gpus': [0,3],
                             'num_nodes': 5},
    'backend_constraints': {'num_processors': 5,
                             'num_gpus': 2,
                             'list_gpus': [1, 2],
                             'num_nodes': 2},
}

job1 = {
    'service_type': 'backend',
    'mlex_app': 'seg-demo',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
    'requirements': {'num_processors': 2,
                     'num_gpus': 1}
}

job2 = {
    'service_type': 'backend',
    'mlex_app': 'mlcoach',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
    'requirements': {'num_processors': 2,
                     'num_gpus': 1}
}

job3 = {
    'service_type': 'frontend',
    'mlex_app': 'clinic',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
    'requirements': {'num_processors': 2}
}

job4 = {
    'service_type': 'backend',
    'mlex_app': 'seg-demo',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
}

workflow1 = {
    'user_uid': '111',
    'description': 'this is a description',
    'job_list': [job1, job2, job3, job4],
    'host_list':['test.als.lbl.gov'],
    'dependencies': {'0': [],
                     '1': [0],
                     '2': [0],
                     '3': [0,2]},
    'requirements': {'num_processors': 2,
                     'num_gpus': 1,
                     'num_nodes': 2}
}


if __name__ == '__main__':
    print('\nSubmitting a host:')
    print('------------------------------------------------------------------------')
    test_submit_host(host1)
    print('\nSubmitting a workflow:')
    print('------------------------------------------------------------------------')
    test_submit_workflow(workflow1)
    print('\nGet next worker:')
    print('------------------------------------------------------------------------')
    test_get_next_workers()
    print('\nCheck dependencies:')
    print('------------------------------------------------------------------------')
    test_dependencies_status()
    print('\nUpdating status:')
    print('------------------------------------------------------------------------')
    test_update_status()
    print('\nTerminating processes:')
    print('------------------------------------------------------------------------')
    test_terminate_processes()
