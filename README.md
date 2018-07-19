# Falcon

The workflow starter of secondary analysis service.

Falcon is currently implemented as a semi single-producer, single/multiple consumer multiprocessing model.


## Development

### Configuration
To make the falcon work properly, you have to either create a `config.json` under `falcon/falcon/config.json`, or
modify the `falcon-dev-compose.yml` file to locate the where the `config.json` is. 

A valid config.json file should look like:
```json
{
    "cromwell_url": "https://cromwell.mint-dev.broadinstitute.org/api/workflows/v1",
    "use_caas": true,
    "cromwell_user": "test",
    "cromwell_password": "test",
    "collection_name": "lira-dev-workflows",
    "queue_update_interval": 10,
    "workflow_start_interval": 1
}
```

**Note:** if you are using Cromwell-as-a-Service with falcon, besides the `config.json`, you also have to provide a valid service account key file `caas_key.json` under `falcon/falcon/config.json` (or change the `falcon-dev-compose.yml` accordingly).

### Build the docker image

To build the docker from the root of the repository with a tag `$TAG`, use:
```bash
docker build -t falcon:$TAG .
```

### Start dev server locally

To run the Falcon in dev mode with docker-compose, which is easier to set up locally, use the following command from the root of the repository:
```bash
docker-compose -f falcon-dev-compose.yml up --build
```

## Testing

To run the tests, from the root of the repository, run:

```bash
cd falcon/test && bash test.sh
```

## To-Do

There are a lot of features and tasks left to be implemented for falcon:

- [ ] Let the Igniter take a rest if it cannot find any workflow to start, to save the computation resource.
- [ ] Implement a mechanism to monitor the statuses of both the Queue Handler and the Igniter, restart them if any of them is in bad status. It can use `process_id` and a process pool to implement this feature.
- [ ] Implement coroutines(possibly using `asyncio`) based igniters.
- [ ] Add more failure handling methods.
- [ ] Implement an adequate integration test suite for falcon.
- [ ] Implement and perform scaling tests for falcon.
- [ ] Write logs into files, while being aware of the weird behavior with `logging` when using multiprocessing.
- [ ] (optional) Integrate falcon with frameworks, to make it accept API calls and improve the availability.
- [ ] (optional) Switch to use short-lived handlers and igniters, instead of long-running processes to improve the performance.
