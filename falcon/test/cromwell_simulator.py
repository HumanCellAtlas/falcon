from unittest.mock import Mock
from requests.models import Response
from uuid import uuid4
import random


def query_workflows_succeed(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        'results': [
            {
                'workflow_id': str(uuid4()), 'submission': '2018-05-25T19:03:51.736Z'
            } for i in range(random.randint(1, 10))
        ]
    }
    return response


def query_workflows_fail_with_400(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 400
    response.json.return_value = {
        'status': 'fail',
        'message': 'An error message for Malformed Request.'
    }
    return response


def query_workflows_fail_with_500(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 500
    response.text.return_value = 'An error message for Internal Server Error.'
    return response


def query_workflows_simulation(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    system_random = random.SystemRandom
    query_func = system_random.choice(
        [query_workflows_succeed for i in range(10)]
            .append(query_workflows_fail_with_400)
            .append(query_workflows_fail_with_500)
    )  # Make the possibilities of getting the status codes 200:400:500 as 10:1:1 in the simulation
    return query_func()


def release_workflow_succeed(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = { "id": workflow_id, "status": "Submitted" }
    return response


def release_workflow_with_400(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    # TODO: figure out when can we get this type of error
    response.status_code = 400
    response.json.return_value = {
        'status': 'fail',
        'message': 'An error message for Malformed Request.'
    }
    return response


def release_workflow_with_403(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        'status': 'error',
        'message': 'Couldn\'t change status of workflow {} to \'Submitted\' because the workflow'
                   ' is not in \'On Hold\' state'.format(workflow_id)
    }
    return response


def release_workflow_with_404(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    # TODO: track on the issue: https://github.com/broadinstitute/cromwell/issues/3911, which causes the Cromwell
    # to return 500 code for 404 errors for now
    response.status_code = 404
    response.json.return_value = {
      'status': 'fail',
      'message': 'Unrecognized workflow ID: {}'.format(workflow_id)
    }
    return response


def release_workflow_with_500(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 500
    response.text.return_value = 'An error message for Internal Server Error.'
    return response


def release_workflow_simulation(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    system_random = random.SystemRandom
    release_func = system_random.choice(
        [release_workflow_succeed for i in range(10)]
            .append(release_workflow_with_400)
            .append(release_workflow_with_403)
            .append(release_workflow_with_404)
            .append(release_workflow_with_500)
    )  # Make the possibilities of getting the status codes 200:400:403:404:500 as 10:1:1:1:1 in the simulation
    return release_func()