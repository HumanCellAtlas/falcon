import json
import random
from unittest.mock import Mock
from uuid import uuid4

import requests
from requests.models import Response


def query_workflows_raises_ConnectionError(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    raise requests.exceptions.ConnectionError


def query_workflows_raises_RequestException(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    raise requests.exceptions.RequestException


def release_workflow_raises_ConnectionError(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    raise requests.exceptions.ConnectionError


def release_workflow_raises_RequestException(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    raise requests.exceptions.RequestException


def query_workflows_succeed(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        'results': [
            {
                'id': str(uuid4()), 'submission': '2018-05-25T19:03:51.736Z'
            } for i in range(random.randint(1, 10))
        ]
    }
    return response


def query_workflows_fail_with_400(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 400
    response.json.return_value = {
        'status' : 'fail',
        'message': 'An error message for Malformed Request.'
    }
    response.text = json.dumps(response.json.return_value)
    return response


def query_workflows_fail_with_500(cromwell_url, query_dict, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 500
    response.text = 'An error message for Internal Server Error.'
    return response


def query_workflows(*args, **kwargs):
    """
    Note: This function monkey-patches the `cromwell-tools.query_workflows()` for simulation purposes, DO NOT use this
    function is unit tests.
    """
    system_random = random.SystemRandom()
    candidate_func_list = [query_workflows_succeed for i in range(20)]
    candidate_func_list.append(query_workflows_fail_with_400)
    candidate_func_list.append(query_workflows_fail_with_500)
    candidate_func_list.append(query_workflows_raises_ConnectionError)
    candidate_func_list.append(query_workflows_raises_RequestException)

    # Make the possibilities of getting the status codes 200:400:500 as 20:1:1 in the simulation
    # There are also 1/12 probabilities the simulator will intentionally throw out some connection issues
    query_func = system_random.choice(
            candidate_func_list
    )
    return query_func(*args, **kwargs)


def release_workflow_succeed(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {"id": workflow_id, "status": "Submitted"}
    response.text = json.dumps(response.json.return_value)
    return response


def release_workflow_with_400(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    # TODO: figure out when can we get this type of error
    response.status_code = 400
    response.json.return_value = {
        'status' : 'fail',
        'message': 'An error message for Malformed Request.'
    }
    response.text = json.dumps(response.json.return_value)
    return response


def release_workflow_with_403(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 403
    response.json.return_value = {
        'status' : 'error',
        'message': 'Couldn\'t change status of workflow {} to \'Submitted\' because the workflow'
                   ' is not in \'On Hold\' state'.format(workflow_id)
    }
    response.text = json.dumps(response.json.return_value)
    return response


def release_workflow_with_404(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    # TODO: track on the issue: https://github.com/broadinstitute/cromwell/issues/3911, which causes the Cromwell
    # to return 500 code for 404 errors for now
    response.status_code = 404
    response.json.return_value = {
        'status' : 'fail',
        'message': 'Unrecognized workflow ID: {}'.format(workflow_id)
    }
    response.text = json.dumps(response.json.return_value)
    return response


def release_workflow_with_500(cromwell_url, workflow_id, cromwell_user, cromwell_password, caas_key):
    response = Mock(spec=Response)
    response.status_code = 500
    response.text = 'An error message for Internal Server Error.'
    return response


def release_workflow(*args, **kwargs):
    """
    Note: This function monkey-patches the `cromwell-tools.release_workflow()` for simulation purposes, DO NOT use this
    function is unit tests.
    """
    system_random = random.SystemRandom()
    candidate_func_list = [release_workflow_succeed for i in range(20)]
    candidate_func_list.append(release_workflow_with_400)
    candidate_func_list.append(release_workflow_with_403)
    candidate_func_list.append(release_workflow_with_404)
    candidate_func_list.append(release_workflow_with_500)
    candidate_func_list.append(release_workflow_raises_ConnectionError)
    candidate_func_list.append(release_workflow_raises_RequestException)

    # Make the possibilities of getting the status codes 200:400:403:404:500 as 20:1:1:1:1 in the simulation
    # There are also 1/13 probabilities the simulator will intentionally throw out some connection issues
    release_func = system_random.choice(
            candidate_func_list
    )
    return release_func(*args, **kwargs)
