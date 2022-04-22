Job manager for MLExchange platfom. To use it, do the following:   

First, create an environmental file (.env), as follows:
```
MONGO_DB_USERNAME=mlex
MONGO_DB_PASSWORD=mlex
COMPOSE_PROJECT_NAME=computing_api
NUM_PROCESSORS=2
HOST={"nickname":"local","hostname":"local.als.lbl.gov","frontend_constraints":{"num_processors":10,"num_gpus":0,"list_gpus":[],"num_nodes":2},"backend_constraints":{"num_processors":5,"num_gpus":0,"list_gpus":[],"num_nodes":2}}
```

Then, `cd mlex_computing_api` and use the command `docker-compose up --build`. 
 
**If you are using Apple M1 machine**, instead use the command `docker-compose -f docker-compose-arm64.yml up --build`
