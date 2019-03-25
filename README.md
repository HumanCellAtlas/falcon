# Falcon

[![Travis (.com)](https://img.shields.io/travis/com/HumanCellAtlas/falcon.svg?label=Unit%20Test%20on%20Travis%20CI%20&style=flat-square)](https://travis-ci.com/HumanCellAtlas/falcon)
[![Docker Repository on Quay](https://quay.io/repository/humancellatlas/secondary-analysis-falcon/status "Docker Repository on Quay")](https://quay.io/repository/humancellatlas/secondary-analysis-falcon)
[![GitHub Release](https://img.shields.io/github/release/HumanCellAtlas/falcon.svg?label=Latest%20Release&style=flat-square&colorB=green)](https://github.com/HumanCellAtlas/falcon/releases)
![Python Version](https://img.shields.io/badge/Python-3.6%20%7C%203.7-green.svg?style=flat-square&logo=python&colorB=blue)
[![License](https://img.shields.io/github/license/HumanCellAtlas/falcon.svg?style=flat-square)](https://github.com/HumanCellAtlas/falcon/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/Code%20Style-black-000000.svg?style=flat-square)](https://github.com/ambv/black)
[![Known Vulnerabilities](https://snyk.io/test/github/HumanCellAtlas/falcon/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/HumanCellAtlas/falcon?targetFile=requirements.txt)

The workflow starter of secondary analysis service.

Falcon is currently implemented following a semi single-producer-single/multiple consumer multi-threading based model.


## Development

### Code Style

The Falcon code base is complying with the PEP-8 and using [Black](https://github.com/ambv/black) to 
format our code, in order to avoid "nitpicky" comments during the code review process so we spend more time discussing about the logic, not code styles.

In order to enable the auto-formatting in the development process, you have to spend a few seconds setting up the `pre-commit` the first time you clone the repo:

1. Install `pre-commit` by running: `pip install pre-commit` (or simply run `pip install -r requirements.txt`).
2. Make sure the `.pre-commit-config.yaml` still looks OK to you.
3. Run `pre-commit install` to install the git hook.

Please make sure you followed the above steps, otherwise your commits might fail at the linting test!

### Configuration
To make the falcon work properly, you have to either create a `config.json` under `falcon/falcon/config.json`, or
modify the `falcon-dev-compose.yml` file to locate where the `config.json` is. 

A valid config.json file should look like:
```json
{
    "cromwell_url": "https://your.cromwell.domain.here",
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
