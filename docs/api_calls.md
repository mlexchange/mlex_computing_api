# Submit workflow

### POST: 

```
http://job-service:8080/api/v0/workflows
```

### JSON Schema:

```
{ 
  "user_uid": "string",
  "job_list": [
    {
      "description": "string",
      "service_type": "backend",
      "mlex_app": "string",
      "job_kwargs": {
        "uri": "string",
        "type": "docker",
        "cmd": "string",
        "map": {},
        "container_kwargs": {},
        "kwargs": {}
      },
      "working_directory": "string"
    }
  ],
  "dependencies": {'0':[]},
  "host_list": [
    "string"
  ],
  "requirements": {
        "num_processors": 0,
        "num_gpus": 0,
        "num_nodes": 1
      }
}
```

Options for service_type: frontend, backend

# Get list of jobs

### GET: 

```
http://job-service:8080/api/v0/jobs
```

### Parameters Schema:

```
{
    "user": "string",
    "host_uid": "string",
    "state": "string"
}
```

Options for state values: queue, running, warning, complete, complete with errors, failed, canceled, terminated

# Get a given job

### GET

```
http://job-service:8080/api/v0/jobs/<job_uid>
```

# Get computing resources constraints from host

### GET

```
http://job-service:8080/api/v0/hosts?&nickname=<host_nickname>
```

# Terminate a running job

### PATCH

```
http://job-service:8080/api/v0/jobs/<job_uid>/terminate
```

# Delete a job

### PATCH

```
http://job-service:8080/api/v0/jobs/<job_uid>/delete
```

