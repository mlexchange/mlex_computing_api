import json
import requests
import time
import urllib

msd_ex1 = {
  'user': 'user1',
  'mlex_app': 'seg-demo',
  'job_type': 'training',
  'description': 'this is a test',
  'deploy_location': 'local',
  'gpu': False,
  'data_uri': '/Users/tanchavez/Documents/Coding/Repositories/segMSDnet/data',
  'container_uri': 'mlexchange/msdnetwork-notebook',
  'container_cmd': 'python3 src/train.py',
  'container_kwargs': {'directories': ['data/mask',
                                       'data/images/train',
                                       'data/output'],
                       'parameters': {'num_epochs': 200,
                                      'optimizer': 'Adam',
                                      'criterion': 'CrossEntropyLoss',
                                      'learning_rate': 0.01,
                                      'num_layers': 10,
                                      'max_dilation': 10}
                       }
}

msd_ex2 = {
  'user': 'user1',
  'mlex_app': 'seg-demo',
  'job_type': 'testing',
  'description': 'this is a test',
  'deploy_location': 'local',
  'gpu': False,
  'data_uri': '/Users/tanchavez/Documents/Coding/Repositories/segMSDnet/data',
  'container_uri': 'mlexchange/msdnetwork-notebook',
  'container_cmd': 'python3 src/segment.py',
  'container_kwargs': {'directories': ['data/images/test/segment_series.tif',
                                       'data/output/state_dict_net.pt',
                                       'data/output'],
                       'parameters': {'show_progress': 20}
                       }
}

url = 'http://localhost:8080/api/v0/jobs'
print(requests.post(url, json=msd_ex1).status_code)

url = 'http://localhost:8080/api/v0/jobs?&user=user1'
response = urllib.request.urlopen(url)
data = json.loads(response.read())
print(data)

url = 'http://localhost:8080/api/v0/jobs/'+data[0]['uid']+'/logs'
response = urllib.request.urlopen(url)
data = json.loads(response.read())
print(data)

time.sleep(60)

url = 'http://localhost:8080/api/v0/jobs'
print(requests.post(url, json=msd_ex2).status_code)
