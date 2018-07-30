import logging
import timeit
from unittest import mock
from unittest.mock import patch

import pytest

from falcon.test import cromwell_simulator
from falcon import queue_handler


def mock_get_settings(path):
    """
    This function mocks the `get_settings()` function, returns a valid dictionary to be consumed.
    """
    return {
        'cromwell_url': 'https://example.cromwell-instance.org/api/workflows/v1',
        'use_caas': False,
        'cromwell_user': 'username',
        'cromwell_password': 'password',
        'queue_update_interval': 60,
        'workflow_start_interval': 10
    }


class TestWorkflow(object):
    """
    This class hosts test cases fro testing the `queue_handler.Workflow` class
    """

    def test_a_workflow_shows_its_own_id_in_logging(self, capsys):
        """
        This function asserts the `Workflow` class implements the `__repr__()` method correctly.

        `capsys` is a fixture of provided by Pytest, which captures all stdout and stderr streams during the test.
        """
        test_workflow = queue_handler.Workflow(
            workflow_id='fake-workflow-1', bundle_uuid='fake-bundle-uuid-1')
        print(test_workflow)

        captured_stdout, _ = capsys.readouterr()
        assert captured_stdout == 'fake-workflow-1\n'

    def test_a_workflow_is_distinguishable_from_another_one(self):
        """
        This function asserts the `Workflow` class implements the `__eq__()` method correctly.

        Note: In the future, if we want to add advanced de-duplication feature to the service, besides assert workflow id
        betweeen 2 Workflow objects, we might also want to check if they have the same bundle_uuid and bundle_version.
        """
        test_workflow1 = queue_handler.Workflow(
            workflow_id='fake-workflow-1', bundle_uuid='fake-bundle-uuid-1')

        test_workflow2 = queue_handler.Workflow(
            workflow_id='fake-workflow-2', bundle_uuid='fake-bundle-uuid-1')

        assert test_workflow1 != test_workflow2


class TestQueue_Handler(object):
    """
    This class hosts all unittest cases for testing the `queue_handler.Queue_Handler` and its methods.
    """

    def test_obtain_queue_returns_a_valid_queue_object(self):
        pass

    def test_queue_handler_can_spawn_and_start_properly(self):
        pass

        

