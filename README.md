Job manager for MLExchange platfom. To use it, do the following:   

First, create an environmental file (.env), as follows:
```
MONGO_DB_USERNAME=your_username
MONGO_DB_PASSWORD=your_password
COMPOSE_PROJECT_NAME=computing_api
```

Then, `cd mlex_computing_api` and use the command `docker-compose up --build`. 
 
**If you are using Apple M1 machine**, instead use the command `docker-compose -f docker-compose-arm64.yml up --build`
