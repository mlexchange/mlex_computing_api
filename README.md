Job manager for MLExchange platfom. 

# Running


To use it, do the following:   

In command line, execute `cd mlex_computing_api`, and  create an environmental file (.env), as follows:
```
MONGO_DB_USERNAME=mlex
MONGO_DB_PASSWORD=mlex
COMPOSE_PROJECT_NAME=computing_api
NUM_PROCESSORS=2
HOST={"nickname":"local","hostname":"local.als.lbl.gov","frontend_constraints":{"num_processors":10,"num_gpus":0,"list_gpus":[],"num_nodes":2},"backend_constraints":{"num_processors":5,"num_gpus":0,"list_gpus":[],"num_nodes":2}}
```


HOST description:
* nickname: Name of host
* hostname: Hostname
* frontend_constraints: Maximum number of resources for frontend services
    * num_processors: Maximum number of processors/cores for frontend services
    * num_gpus: Maximum number of GPUs for frontend services
    * list_gpus: List of GPUs for frontend services, such as [0,1,...]
    * num_nodes: Maximum number of nodes/workers that can be launched at the same time for frontend services
* backend_constraints: Maximum number of resources for backend services
    * num_processors: Maximum number of processors/cores for backend services
    * num_gpus: Maximum number of GPUs for backend services
    * list_gpus: List of GPUs for backend services, such as [2,3,...]
    * num_nodes: Maximum number of nodes/workers that can be launched at the same time for backend services

Make sure to set up these elements according to the resources of your local host.

Once the environmetal file is ready, use the command `docker-compose up --build`. 
 
**If you are using Apple M1 machine**, instead use the command `docker-compose -f docker-compose-arm64.yml up --build`

# Note for developers

The repo includes 1 test script. Before testing, make sure your database is empty (if it is not, delete the database folder). To test, do the following:
* Start the compute service
* Docker exec into the container called job-service
* Go to the test folder: `cd job_manager/api/test`
* Execute `python3 test1.py`

The output of this script summarizes the output of a set of pre-defined tests, they should all be "True". Otherwise, there is an error in the computing service setup. No docker containers will be launched during this test.


