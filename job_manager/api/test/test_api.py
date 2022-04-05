from fastapi.testclient import TestClient
from model import MlexWorker, MlexWorkflow, MlexHost, MlexJob, Status


COMP_URL = 'http://localhost:8080/api/v0/'


def test_submit_host(rest_client:TestClient):
    '''
    This test submits a new host to the database
    Args:
        rest_client: test client
    Returns:
        None
    '''
    response = rest_client.post(f'{COMP_URL}hosts', json=host1)
    assert response.status_code == 200      # host was successfully submitted


def test_submit_workflow(rest_client: TestClient):
    '''
    This test submits a workflow and checks that the amount of workers and jobs matches it's initial description
    Args:
        rest_client:    test client
    Returns:
        None
    '''
    ## submit workflow
    response = rest_client.post(f'{COMP_URL}workflows', json=workflow1)
    workflow_uid = response.json()
    assert response.status_code == 200      # workflow was successfully submitted

    ## retrieve workflow information
    mlex_workflow = rest_client.get(f'{COMP_URL}workflows/{workflow_uid}').json()
    mlex_workflow = MlexWorkflow.parse_obj(mlex_workflow)

    ## retrieve workers uids in workflow
    mlex_workers = []
    for worker_uid in mlex_workflow.workers_list:
        worker = rest_client.get(f'{COMP_URL}workers/{worker_uid}').json()
        mlex_workers.append(worker)

    ## retrieve jobs uids in workflow
    mlex_jobs = []
    for worker in mlex_workers:
        worker = MlexWorker.parse_obj(worker)
        for job_uid in worker.jobs_list:
            job = rest_client.get(COMP_URL+'jobs/'+job_uid).json()
            mlex_jobs.append(job)
    assert len(mlex_jobs) == 4              # there are 4 jobs in this workflow


def test_get_next_workers(rest_client: TestClient):
    '''
    This test retrieves the next worker at certain host and checks if:
        - The worker status has been updated
        - The resources assigned to the worker has been updated at the host
    Args:
        rest_client: Test client
    Returns:
        None
    '''
    # get initial values for comparison
    host = rest_client.get(f'{COMP_URL}hosts').json()
    mlex_host = MlexHost.parse_obj(host)
    init_frontend_available = mlex_host.frontend_available
    init_backend_available = mlex_host.backend_available

    # get next worker
    response = rest_client.get(f'{COMP_URL}private/workers', params={'host_uid': mlex_host.uid,
                                                                     'service_type': 'backend'}).json()
    worker = MlexWorker.parse_obj(response)
    num_processors = worker.requirements.num_processors
    num_gpus = worker.requirements.num_gpus
    assigned_gpus = worker.requirements.list_gpus

    # get final values for comparison
    host = rest_client.get(f'{COMP_URL}hosts').json()
    mlex_host = MlexHost.parse_obj(host)
    final_frontend_available = mlex_host.frontend_available
    final_backend_available = mlex_host.backend_available

    if worker.service_type == 'frontend':
        assert final_frontend_available.num_processors == init_frontend_available.num_processors - num_processors
        assert final_frontend_available.num_gpus == init_frontend_available.num_gpus - num_gpus
        assert init_frontend_available.list_gpus == assigned_gpus + final_frontend_available.list_gpus
        assert init_frontend_available.num_nodes == final_frontend_available.num_nodes + 1

        assert final_backend_available == init_backend_available

    if worker.service_type == 'backend':
        assert final_backend_available.num_processors == init_backend_available.num_processors - num_processors
        assert final_backend_available.num_gpus == init_backend_available.num_gpus - num_gpus
        assert init_backend_available.list_gpus == assigned_gpus + final_backend_available.list_gpus
        assert init_backend_available.num_nodes == final_backend_available.num_nodes + 1

        assert final_frontend_available == init_frontend_available

    if worker.service_type == 'hybrid':
        assert final_frontend_available.num_processors + final_backend_available.num_processors == \
               init_frontend_available.num_processors + init_backend_available.num_processors - num_processors
        assert final_frontend_available.num_gpus + final_backend_available.num_gpus == \
               init_frontend_available.num_gpus + init_backend_available.num_gpus - num_gpus
        assert list(set(init_frontend_available.list_gpus + init_backend_available.list_gpus) ^ \
               set(final_frontend_available.list_gpus + final_backend_available.list_gpus)) == assigned_gpus
        assert init_frontend_available.num_nodes + init_backend_available.num_nodes ==\
               final_frontend_available.num_nodes + final_backend_available.num_nodes + 1
    assert worker.host_uid == mlex_host.uid and worker.status.state == 'running'


def test_update_status(rest_client: TestClient):
    # get next worker
    host = rest_client.get(f'{COMP_URL}hosts').json()
    mlex_host = MlexHost.parse_obj(host)
    response = rest_client.get(f'{COMP_URL}private/workers', params={'host_uid': mlex_host.uid,
                                                                     'service_type': 'hybrid'}).json()
    worker = MlexWorker.parse_obj(response)
    # change the status of the jobs in this worker
    for job_uid in worker.jobs_list:
        # change the status
        response = rest_client.get(f'{COMP_URL}private/jobs/{job_uid}')
        assert response.status_code == 200
        response = rest_client.patch(f'{COMP_URL}private/jobs/{job_uid}/update',
                                   params={'logs': 'this is a test'})
        assert response.status_code == 200
        # check that the status has been changed correctly
        job = rest_client.get(f'{COMP_URL}jobs/{job_uid}').json()
        mlex_job = MlexJob.parse_obj(job)
        assert mlex_job.status.state == 'running' and mlex_job.logs == 'this is a test'
    # check that the status has changed too
    item = rest_client.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    assert mlex_item.status.state == 'running'

    # let's change the status of the last job as failed
    status = Status(**{'state': 'failed', 'return_code': 'Error 1234'})
    response = rest_client.patch(f'{COMP_URL}private/jobs/{job_uid}/update', json=status.dict())
    assert response.status_code == 200
    # check that the status has been changed correctly
    job = rest_client.get(f'{COMP_URL}jobs/{job_uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    assert mlex_job.status.state == 'failed' and mlex_job.status.return_code == 'Error 1234'
    # check that the worker status has changed too
    item = rest_client.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    assert mlex_item.status.state == 'warning'

    # let's mark the first job as completed
    first_job_uid = worker.jobs_list[0]
    status = Status(**{'state': 'complete'})
    response = rest_client.patch(f'{COMP_URL}private/jobs/{first_job_uid}/update', json=status.dict())
    assert response.status_code == 200
    # check that the status has been changed correctly
    job = rest_client.get(f'{COMP_URL}jobs/{first_job_uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    assert mlex_job.status.state == 'complete'
    # check that the status has changed too
    item = rest_client.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    assert mlex_item.status.state == 'complete with errors'

    # check the status of the workflow
    workflow = rest_client.get(f'{COMP_URL}workflows').json()
    assert len(workflow) == 1
    mlex_workflow = MlexWorkflow.parse_obj(workflow[0])
    assert mlex_workflow.status.state == 'warning'


def test_terminate_processes(rest_client: TestClient):
    status = Status(**{'state': 'queue'})
    jobs = rest_client.get(f'{COMP_URL}jobs', json=status.dict()).json()
    mlex_jobs = []
    for job in jobs:
        mlex_jobs.append(MlexJob.parse_obj(job))
    assert len(mlex_jobs) == 2       # the jobs in this worker have not been executed yet

    # terminate the first job
    first_job_uid = mlex_jobs[0].uid
    response = rest_client.patch(f'{COMP_URL}jobs/{first_job_uid}/terminate')
    assert response.status_code == 200
    job = rest_client.get(f'{COMP_URL}jobs/{first_job_uid}').json()
    mlex_job = MlexJob.parse_obj(job)
    assert mlex_job.terminate

    # terminate the worker
    status = Status(**{'state': 'running'})
    worker = rest_client.get(f'{COMP_URL}workers', json=status.dict()).json()
    assert len(worker) == 1  # worker 1 is still running
    worker = MlexWorker.parse_obj(worker[0])
    response = rest_client.patch(f'{COMP_URL}workers/{worker.uid}/terminate')
    assert response.status_code == 200
    item = rest_client.get(f'{COMP_URL}workers/{worker.uid}').json()
    mlex_item = MlexWorker.parse_obj(item)
    assert mlex_item.terminate

    # terminate workflow
    workflow = rest_client.get(f'{COMP_URL}workflows').json()
    assert len(workflow) == 1
    mlex_workflow = MlexWorkflow.parse_obj(workflow[0])
    response = rest_client.patch(f'{COMP_URL}workflows/{mlex_workflow.uid}/terminate')
    assert response.status_code == 200
    item = rest_client.get(f'{COMP_URL}workflows/{mlex_workflow.uid}').json()
    mlex_item = MlexWorkflow.parse_obj(item)
    assert mlex_item.terminate

#################################################### TEST ELEMENTS ####################################################
job1 = {
    'service_type': 'backend',
    'mlex_app': 'seg-demo',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
}

job2 = {
    'service_type': 'backend',
    'mlex_app': 'mlcoach',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
}

job3 = {
    'service_type': 'frontend',
    'mlex_app': 'clinic',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
}

job4 = {
    'service_type': 'backend',
    'mlex_app': 'seg-demo',
    'job_kwargs': {'uri': 'image', 'cmd': 'python3'},
    'working_directory': 'home',
}

workflow1 = {
    'user_uid': '111',
    'workflow_type': 'serial',
    'job_list': [job1, job2, job3, job4],
    'requirements': {'num_processors': 2,
                     'num_gpus': 1,
                     'num_nodes': 2}
}

host1 = {
    'nickname': 'vaughan',
    'hostname': 'vaughan.als.lbl.gov',
    'frontend_constraints': {'num_processors': 10,
                             'num_gpus': 2,
                             'list_gpus': [0,3],
                             'num_nodes': 5},
    'backend_constraints': {'num_processors': 5,
                             'num_gpus': 2,
                             'list_gpus': [1, 2],
                             'num_nodes': 2},
}
