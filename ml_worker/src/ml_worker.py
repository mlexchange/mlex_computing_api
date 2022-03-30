import argparse
import json
import time

import docker
import requests
import urllib

from model import MlexWorker, MlexJob


COMP_API_URL = 'http://job-service:8080/api/v0/'
DOCKER_CLIENT = docker.from_env()


def get_job(job_uid):
    response = urllib.request.urlopen(f'{COMP_API_URL}jobs/{job_uid}')
    job = json.loads(response.read())
    return MlexJob.parse_obj(job)


def update_job_status(job_id, status):
    response = requests.patch(f'{COMP_API_URL}private/jobs/{job_id}/update', params={'status': status})
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker', help='worker description')
    args = parser.parse_args()

    # get worker information
    worker = MlexWorker(**json.loads(args.worker))
    num_processors = worker.requirements.num_processors
    list_gpus = worker.requirements.list_gpus

    for job_uid in worker.jobs_list:
        new_job = get_job(job_uid)
        try:
            # launch job
            docker_job = new_job.job_kwargs
            cmd = docker_job.cmd
            volumes = []
            device_requests = []
            if len(new_job.working_directory)>0:
                volumes = ['{}:/app/work/data'.format(new_job.working_directory)]
            if len(list_gpus)>0:
                device_requests=[docker.types.DeviceRequest(device_ids=list_gpus,
                                                            capabilities=[['gpu']]
                                                            )],
            container = DOCKER_CLIENT.containers.run(docker_job.uri,
                                                     cpu_count=num_processors,
                                                     device_requests=device_requests,
                                                     command=cmd,
                                                     volumes=volumes,
                                                     detach=True)
        except Exception as err:
            print(err)
            update_job_status(new_job.uid, 'failed')
        else:
            while container.status == 'created' or container.status == 'running':
                new_job = get_job(job_uid)
                if new_job.terminate:
                    container.kill()
                    update_job_status(new_job.uid, 'terminated')
                else:
                    try:
                        output = container.logs(stdout=True)
                        # svc_context.job_svc.update_logs(new_job.uid, output) --> notify me
                    except Exception as err:
                        update_job_status(new_job.uid, 'failed')
                time.sleep(1)
                container = DOCKER_CLIENT.containers.get(container.id)
            result = container.wait()
            if result["StatusCode"] == 0:
                output = container.logs(stdout=True)
                #### get the output
                # svc_context.job_svc.update_logs(new_job.uid, output)  --> me
                update_job_status(new_job.uid, 'complete')
            else:
                if new_job.terminate is None:
                    try:
                        output = container.logs(stdout=True)
                        # svc_context.job_svc.update_logs(new_job.uid, output)  --> notify me
                    except Exception:
                        pass
                    err = "Code: "+str(result["StatusCode"])+ " Error: " + repr(result["Error"])
                    update_job_status(new_job.uid, 'failed')
        # container.remove()
