import requests


COMP_URL = 'http://localhost:8080/api/v0/'


def submit_local_host():
    '''
    This function submits the local host
    '''
    response = requests.post(f'{COMP_URL}hosts', json=host1)
    assert response.status_code == 200      # host was successfully submitted


def submit_workflow():
    '''
    This function submits the workflow to the computing api
    '''
    response = requests.post(f'{COMP_URL}workflows', json=workflow1)
    assert response.status_code == 200      # workflow was successfully submitted


#################################################### TEST ELEMENTS ####################################################
job1 = {
    'mlex_app': 'seg-demo',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc',
                   'cmd': 'python kmeans.py data/images/segment_series.tif data/model \'{"n_clusters":2, "max_iter":300}\''},
    'working_directory': '/Users/tanchavez/Documents/Coding/Repositories/Direct/mlex_kmeans/data',
}

job2 = {
    'mlex_app': 'mlcoach',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 30'},
    'working_directory': '',
}

job3 = {
    'mlex_app': 'data-clinic',
    'job_kwargs': {'uri': 'mlexchange/k-means-dc', 'cmd': 'sleep 45'},
    'working_directory': '',
}

workflow1 = {
    'user_uid': '001',
    'workflow_type': 'serial',
    'job_list': [job1, job2, job3],
    'requirements': {'num_processors': 2,
                     'num_gpus': 0,
                     'num_nodes': 2}
}

host1 = {
    'nickname': 'local',
    'hostname': 'local.als.lbl.gov',
    'max_nodes': 2,
    'max_processors': 10,
    'max_gpus': 0,
    'num_available_processors': 10,
    'num_available_gpus': 0,
    'list_available_gpus': [],
    'num_running_workers': 0
}
