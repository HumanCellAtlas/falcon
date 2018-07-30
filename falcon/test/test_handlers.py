import logging
import timeit
from unittest import mock
from unittest.mock import patch
from queue import Queue
import queue

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


@mock.create_autospec
def mock_queue_handler_execution(self):
    """
    This function mocks the `igniter.execution()` instance method, it doesn't have any functionality.
    The motivation of mocking this is to avoid executing the actual while loop in `queue_handler.execution()`
     during the unittest.
    """
    return True


@mock.create_autospec
def mock_queue_handler_retrieve_workflows(self, query_dict):
    results = [
        {
            'id': 'fake-id-1',
            'name': 'fake-name-1',
            'status': 'On Hold',
            'submission': '2018-01-01T23:49:40.620Z',
            'labels': {
                'cromwell-workflow-id': 'cromwell-fake-id-1',
                'bundle-uuid': 'fake-bundle-uuid-1',
                'bundle-version': '2018-01-01T22:49:40.620Z',
                'workflow-name': 'fake-name-1'
            }
        },
        {
            'id': 'fake-id-2',
            'name': 'fake-name-2',
            'status': 'On Hold',
            'submission': '2018-01-02T23:49:40.620Z',
            'labels': {
                'cromwell-workflow-id': 'cromwell-fake-id-2',
                'bundle-uuid': 'fake-bundle-uuid-2',
                'bundle-version': '2018-01-01T22:49:40.620Z',
                'workflow-name': 'fake-name-2'
            }
        },
        {
            'id': 'fake-id-3',
            'name': 'fake-name-3',
            'status': 'On Hold',
            'submission': '2018-01-03T23:49:40.620Z',
            'labels': {
                'cromwell-workflow-id': 'cromwell-fake-id-3',
                'bundle-uuid': 'fake-bundle-uuid-3',
                'bundle-version': '2018-01-01T22:49:40.620Z',
                'workflow-name': 'fake-name-3'
            }
        }
    ]
    return results


@mock.create_autospec
def mock_queue_handler_retrieve_no_workflow(self, query_dict):
    results = []
    return results


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
        q = queue_handler.Queue_Handler.obtainQueue()
        assert isinstance(q, Queue)

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch.object(queue_handler.Queue_Handler, 'execution', new=mock_queue_handler_execution)
    def test_queue_handler_can_spawn_and_start_properly(self):
        test_handler = queue_handler.Queue_Handler('mock_path')
        try:
            test_handler.spawn_and_start()
            mock_queue_handler_execution.assert_called_once_with(test_handler)
        finally:
            test_handler.thread.join()

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        test_handler = queue_handler.Queue_Handler('mock_path')
        test_sleep_time = 1

        start = timeit.default_timer()
        test_handler.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_queue_handler_join_can_handle_exception(self, caplog):
        caplog.set_level(logging.ERROR)
        test_handler = queue_handler.Queue_Handler('mock_path')

        assert test_handler.thread is None

        test_handler.join()
        error = caplog.text

        assert 'The thread of this queue handler is not in a running state.' in error

    def test_is_workflow_list_in_oldest_first_order_function_returns_true_on_oldest_first_workflow_list(self):
        oldest_first_workflow_list = [
            {
                'id': 'fake-id-1',
                'name': 'fake-name-1',
                'status': 'On Hold',
                'submission': '2018-01-01T23:49:40.620Z'
            },
            {
                'id': 'fake-id-2',
                'name': 'fake-name-2',
                'status': 'On Hold',
                'submission': '2018-01-02T23:49:40.620Z'
            },
            {
                'id': 'fake-id-3',
                'name': 'fake-name-3',
                'status': 'On Hold',
                'submission': '2018-01-03T23:49:40.620Z'
            }
        ]

        assert queue_handler.Queue_Handler.is_workflow_list_in_oldest_first_order(
            oldest_first_workflow_list
        ) is True

    def test_is_workflow_list_in_oldest_first_order_function_returns_false_on_newest_first_workflow_list(self):
        newest_first_workflow_list = [
            {
                'id': 'fake-id-1',
                'name': 'fake-name-1',
                'status': 'On Hold',
                'submission': '2018-01-03T23:49:40.620Z'
            },
            {
                'id': 'fake-id-2',
                'name': 'fake-name-2',
                'status': 'On Hold',
                'submission': '2018-01-02T23:49:40.620Z'
            },
            {
                'id': 'fake-id-3',
                'name': 'fake-name-3',
                'status': 'On Hold',
                'submission': '2018-01-01T23:49:40.620Z'
            }
        ]

        assert queue_handler.Queue_Handler.is_workflow_list_in_oldest_first_order(
            newest_first_workflow_list
        ) is False

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_succeed, create=True)
    def test_retrieve_workflows_returns_query_results_successfully(self):
        test_handler = queue_handler.Queue_Handler('mock_path')
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        assert isinstance(results, list)
        assert len(results) > 0

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_fail_with_500,
           create=True)
    def test_retrieve_workflows_returns_empty_list_on_exceptions(self, caplog):
        caplog.set_level(logging.WARNING)
        test_handler = queue_handler.Queue_Handler('mock_path')
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        warn = caplog.text

        assert isinstance(results, list)
        assert len(results) == 0
        assert 'Failed to retrieve workflows from Cromwell' in warn

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_indeed_rebuilds_a_new_workflow_queue_and_changes_the_reference_pointer_properly(self, caplog):
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.Queue_Handler('mock_path')
        initial_queue_id = id(test_handler.workflow_queue)
        mock_results = mock_queue_handler_retrieve_workflows(test_handler, test_handler.cromwell_query_dict)
        mock_counts = len(mock_results)

        test_handler.enqueue(mock_results)

        final_queue_id = id(test_handler.workflow_queue)

        info = caplog.text
        # assert 'Retrieved {0} workflows from Cromwell.'.format(mock_counts) in info
        assert 'Retrieved 3 workflows from Cromwell.' in info
        assert initial_queue_id != final_queue_id

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_can_put_workflow_objects_to_the_new_workflow_queue(self, caplog):
        caplog.set_level(logging.DEBUG)
        test_handler = queue_handler.Queue_Handler('mock_path')
        mock_results = mock_queue_handler_retrieve_workflows(test_handler, test_handler.cromwell_query_dict)

        test_handler.enqueue(mock_results)

        debug = caplog.text
        assert 'Enqueuing workflow fake-id-1' in debug
        assert 'Enqueuing workflow fake-id-2' in debug
        assert 'Enqueuing workflow fake-id-3' in debug

        try:
            wf1 = test_handler.workflow_queue.get()
            wf2 = test_handler.workflow_queue.get()
            wf3 = test_handler.workflow_queue.get()
        except queue.Empty:
            assert False

        assert wf1.id == 'fake-id-1'
        assert wf2.id == 'fake-id-2'
        assert wf3.id == 'fake-id-3'

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_goes_back_to_sleep_when_no_workflow_is_retrieved(self, caplog):
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.Queue_Handler('mock_path')
        mock_results = mock_queue_handler_retrieve_no_workflow(test_handler, test_handler.cromwell_query_dict)

        test_handler.enqueue(mock_results)

        info = caplog.text

        assert 'Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt.' in info
