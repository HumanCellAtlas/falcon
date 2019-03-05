# Falcon

[![Build Status](https://travis-ci.com/HumanCellAtlas/falcon.svg?branch=master)](https://travis-ci.com/HumanCellAtlas/falcon)
[![Docker Repository on Quay](https://quay.io/repository/humancellatlas/secondary-analysis-falcon/status "Docker Repository on Quay")](https://quay.io/repository/humancellatlas/secondary-analysis-falcon)

The workflow starter of secondary analysis service.

Falcon is currently implemented following a semi single-producer-single/multiple consumer multi-threading based model.


## Development

### Configuration
To make the falcon work properly, you have to either create a `config.json` under `falcon/falcon/config.json`, or
modify the `falcon-dev-compose.yml` file to locate where the `config.json` is. 

A valid config.json file should look like:
```json
{
    "cromwell_url": "https://your.cromwell.domain.here/api/workflows/v1",
    "use_caas": false,
    "cromwell_user": "test",
    "cromwell_password": "test",
    "collection_name": "collection-name",
    "queue_update_interval": 60,
    "workflow_start_interval": 10
}
```

To change the workflows that are started by Falcon, optionally specify a `cromwell_query_dict` in the `config.json`:
```json
{
    "cromwell_query_dict": {
        "status": "On Hold",
        "label": {
            "comment": "scale-test-workflow"
        }
    }
}
```

**Note:** if you are using Cromwell-as-a-Service with falcon, besides the `config.json`, you also have to provide a valid service account key file `caas_key.json` under `falcon/falcon/config.json` (or change the `falcon-dev-compose.yml` accordingly).

### Build the docker image

To build the docker from the root of the repository with a tag `$TAG`, use:
```bash
docker build -t falcon:$TAG .
```

### Start dev server locally

To run the Falcon in `develop` mode with docker-compose, which is easier to set up locally, use the following command from the root of the repository:
```bash
docker-compose -f falcon-dev-compose.yml up --build
```

### Simulation
Falcon comes with a light-weight Cromwell simulator, which provides a basic set of funtions that simulates all possible responses from a real Cromwell, this will only be helpful if you want to make a lot of changes to the Falcon code base. 

To run the simulation, you have to:

1. Go to both `queue_handler.py` and `igniter.py` and replace `from cromwell_tools.cromwell_api import CromwellAPI` with
`from falcon.test import cromwell_simulator as CromwellAPI`.
2. Start Falcon in develop mode, e.g. from the root of the repository:
    ```bash
    docker-compose -f falcon-dev-compose.yml up --build
    ```

## Testing

The test cases are written with [Pytest](https://docs.pytest.org/en/latest/), to run the tests, from the root of the repository, run:

```bash
cd falcon/test && bash test.sh
```

## To-Do

There are a lot of features and tasks left to be implemented for falcon:

- [ ] Dynamically let the Igniter take a rest if it cannot find any workflow to start, to save the computation resource.
- [ ] Implement a mechanism to monitor the statuses of both the Queue Handler and the Igniter, restart them if any of them is in bad status. It can use `Thread.get_ident()` and a thread pool to implement this feature.
- [ ] Implement coroutine(possibly using `asyncio`)-based igniters.
- [ ] Implement and perform scaling tests for falcon.
- [ ] Implement health checks, might be helpful to take advantage of those existing tools, like Kubernetes's probes.
- [ ] (optional) Write logs into files.
- [ ] (optional) Integrate falcon with frameworks, to make it accept API calls and improve the availability.
- [ ] (optional) Switch to use short-lived handlers and igniters, instead of long-running threads to improve the performance.
