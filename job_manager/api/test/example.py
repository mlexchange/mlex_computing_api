import requests


COMP_URL = 'http://localhost:8080/api/v0/'


def submit_local_host():
    '''
    This function submits the local host
    '''
    response = requests.post(f'{COMP_URL}hosts', json=host1)
    assert response.status_code == 200      # host was successfully submitted


def submit_workflow(workflow):
    '''
    This function submits the workflow to the computing api
    '''
    response = requests.post(f'{COMP_URL}workflows', json=workflow)
    assert response.status_code == 200      # workflow was successfully submitted


#################################################### TEST ELEMENTS ####################################################
job1 = {
    'mlex_app': 'seg-demo',
    'service_type': 'backend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc',
                   'cmd': 'python kmeans.py data/images/segment_series.tif data/model \'{"n_clusters":2, "max_iter":300}\''},
    'working_directory': '/Users/tanchavez/Documents/Coding/Repositories/Direct/mlex_kmeans/data',
}

job2 = {
    'mlex_app': 'mlcoach',
    'service_type': 'backend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 30'},
    'working_directory': '',
}

job3 = {
    'mlex_app': 'data-clinic',
    'service_type': 'backend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 45'},
    'working_directory': '',
}

job4 = {
    'mlex_app': 'app4',
    'service_type': 'backend',
    'job_kwargs': {'uri': 'mlexchange/colorwheel-notebook:latest', 'cmd': 'python src/frontend.py', 'port': [8061]},
    'working_directory': '/Users/tanchavez/Documents/Coding/Repositories/Forks/mlex_colorwheel/data',
}

job5 = {
    'mlex_app': 'app5',
    'service_type': 'frontend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 300'},
    'working_directory': '',
}

job6 = {
    'mlex_app': 'app4',
    'service_type': 'backend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 300'},
    'working_directory': '',
}

job7 = {
    'mlex_app': 'app1',
    'service_type': 'frontend',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 300'},
    'working_directory': '',
}

job8 = {
    'mlex_app': 'app1',
    'service_type': 'frontend',
    'job_kwargs': {'uri': 'mlex_colorwheel_colorwheel', 'cmd': 'python src/frontend.py', 'port': [8050]},
    'working_directory': '/data/tanchavez/Repositories/Forks/',
}

workflow1 = {
    'user_uid': '001',
    'workflow_type': 'serial',
    'job_list': [job1, job2, job3],
    'requirements': {'num_processors': 2,
                     'num_gpus': 0,
                     'num_nodes': 2}
}

workflow2 = {
    'user_uid': '002',
    'workflow_type': 'serial',
    'job_list': [job4, job5, job6, job7],
    'requirements': {'num_processors': 2,
                     'num_gpus': 0,
                     'num_nodes': 2}
}

workflow3 = {
    'user_uid': '002',
    'workflow_type': 'serial',
    'job_list': [job8],
    'requirements': {'num_processors': 2,
                     'num_gpus': 1,
                     'num_nodes': 1}
}

host1 = {
    'nickname': 'local',
    'hostname': 'local.als.lbl.gov',
    'frontend_constraints': {'num_processors': 10,
                             'num_gpus': 0,
                             'list_gpus': [],
                             'num_nodes': 2},
    'backend_constraints': {'num_processors': 5,
                             'num_gpus': 0,
                             'list_gpus': [],
                             'num_nodes': 2},
}
