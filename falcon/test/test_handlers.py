import logging
import os
import timeit
from queue import Queue
from unittest import mock
from unittest.mock import patch

import pytest

from falcon import queue_handler
from falcon.test import cromwell_simulator


@mock.create_autospec
def mock_queue_handler_execution_loop(self):
    """
    This function mocks the `queue_handler.execution_loop()` instance method, it doesn't have any functionality.
    The motivation of mocking this is to avoid executing the actual while loop in `queue_handler.execution_loop()`
    during the unittest.
    """
    return True


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

        Note: In the future, if we want to add advanced de-duplication feature to the service, besides asserting
        workflow id between 2 Workflow objects, we might also want to check if they have the same bundle_uuid and
        bundle_version.
        """
        test_workflow1 = queue_handler.Workflow(
                workflow_id='fake-workflow-1', bundle_uuid='fake-bundle-uuid-1')

        test_workflow2 = queue_handler.Workflow(
                workflow_id='fake-workflow-2', bundle_uuid='fake-bundle-uuid-1')

        assert test_workflow1 != test_workflow2


class TestQueueHandler(object):
    """
    This class hosts all unittest cases for testing the `queue_handler.QueueHandler` and its methods. This class takes
    advantages of monkey-patch, some mock features and pytest fixtures:

    `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

    `capsys` is a fixture of provided by Pytest, which captures all stdout and stderr streams during the test.

    `@pytest.mark.timeout()` limits the maximum running time for test cases.

    `@patch`s in the class monkey patches the functions that need to talk to an external resource, so that
    we can test the igniter without actually talking to the external resource, in this case, the Cromwell API.
    """

    data_dir = '{}/data/'.format(os.path.split(__file__)[0])
    cromwell_config = 'example_config_cromwell_instance.json'
    config_path = '{0}{1}'.format(data_dir, cromwell_config)
    mock_workflow_metas = [
        {
            'id'        : 'fake-id-1',
            'name'      : 'fake-name-1',
            'status'    : 'On Hold',
            'submission': '2018-01-01T23:49:40.620Z',
            'labels'    : {
                'cromwell-workflow-id': 'cromwell-fake-id-1',
                'bundle-uuid'         : 'fake-bundle-uuid-1',
                'bundle-version'      : '2018-01-01T22:49:40.620Z',
                'workflow-name'       : 'fake-name-1'
            }
        },
        {
            'id'        : 'fake-id-2',
            'name'      : 'fake-name-2',
            'status'    : 'On Hold',
            'submission': '2018-01-02T23:49:40.620Z',
            'labels'    : {
                'cromwell-workflow-id': 'cromwell-fake-id-2',
                'bundle-uuid'         : 'fake-bundle-uuid-2',
                'bundle-version'      : '2018-01-01T22:49:40.620Z',
                'workflow-name'       : 'fake-name-2'
            }
        },
        {
            'id'        : 'fake-id-3',
            'name'      : 'fake-name-3',
            'status'    : 'On Hold',
            'submission': '2018-01-03T23:49:40.620Z',
            'labels'    : {
                'cromwell-workflow-id': 'cromwell-fake-id-3',
                'bundle-uuid'         : 'fake-bundle-uuid-3',
                'bundle-version'      : '2018-01-01T22:49:40.620Z',
                'workflow-name'       : 'fake-name-3'
            }
        }
    ]

    def test_create_empty_queue_returns_a_valid_empty_queue_object(self):
        """
        This function asserts the `queue_handler.create_empty_queue()` returns a valid `queue.Queue` object and it is
        empty.
        """
        q = queue_handler.QueueHandler.create_empty_queue()
        assert isinstance(q, Queue)
        assert q.empty() is True

    @patch.object(queue_handler.QueueHandler, 'execution_loop', new=mock_queue_handler_execution_loop)
    def test_queue_handler_can_spawn_and_start_properly(self):
        """
        This function asserts the `queue_handler.spawn_and_start()` can be executed properly.
        """
        test_handler = queue_handler.QueueHandler(self.config_path)
        try:
            test_handler.spawn_and_start()
            mock_queue_handler_execution_loop.assert_called_once_with(test_handler)
        finally:
            test_handler.thread.join()

    @pytest.mark.timeout(2)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        """
        This function asserts the `queue_handler.sleep_for()` pauses the thread for at least a given duration.
        """
        test_handler = queue_handler.QueueHandler(self.config_path)
        test_sleep_time = 1

        start = timeit.default_timer()
        test_handler.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    def test_queue_handler_join_can_handle_exception(self, caplog):
        """
        This function asserts the `queue_handler.join()` handles the exception properly, meanwhile, insufficiently, this
        to some extent, tests the availability of `queue_handler.join()`, since it's just a wrapper around the
        `threading.Thread.join()`.
        """
        caplog.set_level(logging.ERROR)
        test_handler = queue_handler.QueueHandler(self.config_path)

        assert test_handler.thread is None

        test_handler.join()
        error = caplog.text

        assert 'The thread of this queue handler is not in a running state.' in error

    def test_is_workflow_list_in_oldest_first_order_function_returns_true_on_oldest_first_workflow_list(self):
        """
        This function asserts the static method `is_workflow_list_in_oldest_first_order()` returns `True` if the
        input list of workflows are sorted in oldest-first order on the `submission` field.
        """
        oldest_first_workflow_list = [
            {
                'id'        : 'fake-id-1',
                'name'      : 'fake-name-1',
                'status'    : 'On Hold',
                'submission': '2018-01-01T23:49:40.620Z'
            },
            {
                'id'        : 'fake-id-2',
                'name'      : 'fake-name-2',
                'status'    : 'On Hold',
                'submission': '2018-01-02T23:49:40.620Z'
            },
            {
                'id'        : 'fake-id-3',
                'name'      : 'fake-name-3',
                'status'    : 'On Hold',
                'submission': '2018-01-03T23:49:40.620Z'
            }
        ]

        assert queue_handler.QueueHandler.is_workflow_list_in_oldest_first_order(
                oldest_first_workflow_list
        ) is True

    def test_is_workflow_list_in_oldest_first_order_function_returns_false_on_newest_first_workflow_list(self):
        """
        This function asserts the static method `is_workflow_list_in_oldest_first_order()` returns `False` if the
        input list of workflows are sorted in newest-first order on the `submission` field.
        """
        newest_first_workflow_list = [
            {
                'id'        : 'fake-id-1',
                'name'      : 'fake-name-1',
                'status'    : 'On Hold',
                'submission': '2018-01-03T23:49:40.620Z'
            },
            {
                'id'        : 'fake-id-2',
                'name'      : 'fake-name-2',
                'status'    : 'On Hold',
                'submission': '2018-01-02T23:49:40.620Z'
            },
            {
                'id'        : 'fake-id-3',
                'name'      : 'fake-name-3',
                'status'    : 'On Hold',
                'submission': '2018-01-01T23:49:40.620Z'
            }
        ]

        assert queue_handler.QueueHandler.is_workflow_list_in_oldest_first_order(
                newest_first_workflow_list
        ) is False

    def test_assemble_workflow_can_work_on_workflow_metadata_properly(self):
        """
        This function asserts the `queue_handler._assemble_workflow()` properly parses an object of workflow metadata
        and assemble it as a `Workflow` instance.
        """
        test_metadata = {
            'id'        : 'fake-id-1',
            'name'      : 'fake-name-1',
            'status'    : 'On Hold',
            'submission': '2018-01-01T23:49:40.620Z',
            'labels'    : {
                'cromwell-workflow-id': 'cromwell-fake-id-1',
                'bundle-uuid'         : 'fake-bundle-uuid-1',
                'bundle-version'      : '2018-01-01T22:49:40.620Z',
                'workflow-name'       : 'fake-name-1'
            }
        }
        test_handler = queue_handler.QueueHandler(self.config_path)
        workflow = test_handler._assemble_workflow(test_metadata)

        assert isinstance(workflow, queue_handler.Workflow)
        assert workflow.id == 'fake-id-1'
        assert workflow.bundle_uuid == 'fake-bundle-uuid-1'
        assert workflow.bundle_version == '2018-01-01T22:49:40.620Z'

    def test_set_queue_indeed_changes_the_reference_pointer_properly(self, caplog):
        """
        This function asserts the `queue_handler.set_queue()` accepts a `queue.Queue` object and points the reference
        to the queue when it gets called.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler(self.config_path)
        initial_queue_id = id(test_handler.workflow_queue)

        another_queue = Queue(-1)
        another_queue_id = id(another_queue)
        test_handler.set_queue(another_queue)

        final_queue_id = id(test_handler.workflow_queue)

        assert initial_queue_id != final_queue_id
        assert final_queue_id == another_queue_id

    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_succeed,
           create=True)
    def test_retrieve_workflows_returns_query_results_successfully(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly when it gets 200 OK from
        the Cromwell.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler(self.config_path)
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        info = caplog.text

        assert isinstance(results, list)
        num_workflows = len(results)
        assert num_workflows > 0
        assert 'Retrieved {0} workflows from Cromwell.'.format(num_workflows) in info

    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_fail_with_500,
           create=True)
    def test_retrieve_workflows_returns_none_for_500_response_code(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly when it gets 500 error code from
        the Cromwell.
        """
        caplog.set_level(logging.WARNING)
        test_handler = queue_handler.QueueHandler(self.config_path)
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        warn = caplog.text

        assert results is None
        assert 'Failed to retrieve workflows from Cromwell' in warn

    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_fail_with_400,
           create=True)
    def test_retrieve_workflows_returns_none_for_400_response_code(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly when it gets 400 error code from
        the Cromwell.
        """
        caplog.set_level(logging.WARNING)
        test_handler = queue_handler.QueueHandler(self.config_path)
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        warn = caplog.text

        assert results is None
        assert 'Failed to retrieve workflows from Cromwell' in warn

    @patch('falcon.queue_handler.cromwell_tools.query_workflows',
           cromwell_simulator.query_workflows_raises_ConnectionError,
           create=True)
    def test_retrieve_workflows_returns_none_for_connection_error(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly if it runs into connection
        errors when talking to the Cromwell.
        """
        caplog.set_level(logging.ERROR)
        test_handler = queue_handler.QueueHandler(self.config_path)
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        error = caplog.text

        assert results is None
        assert 'Failed to retrieve workflows from Cromwell' in error

    @patch('falcon.queue_handler.cromwell_tools.query_workflows',
           cromwell_simulator.query_workflows_raises_RequestException,
           create=True)
    def test_retrieve_workflows_returns_none_for_400_requests_exception(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly if it runs into requests
        exceptions when talking to
        the Cromwell.
        """
        caplog.set_level(logging.ERROR)
        test_handler = queue_handler.QueueHandler(self.config_path)
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        error = caplog.text

        assert results is None
        assert 'Failed to retrieve workflows from Cromwell' in error

    def test_enqueue_can_put_a_workflow_into_the_queue(self):
        """
        This function asserts the `queue_handler.enqueue()` puts a workflow into the queue
        """
        test_handler = queue_handler.QueueHandler(self.config_path)
        assert test_handler.workflow_queue.empty() is True

        mock_workflow = queue_handler.Workflow(
                workflow_id='fake_workflow_id',
                bundle_uuid='fake_bundle_uuid',
                bundle_version='fake_bundle_version')
        test_handler.enqueue(iter([mock_workflow]))
        assert test_handler.workflow_queue.empty() is False

        out = test_handler.workflow_queue.get()
        assert out.id == 'fake_workflow_id'

    def test_prepare_workflows_returns_a_workflow_iterator_correctly(self):
        """
        This function asserts the `queue_handler.prepare_workflows()` returns an expected iterator of the list of
        `Workflow` objects.
        """
        test_handler = queue_handler.QueueHandler(self.config_path)
        test_iterator = test_handler.prepare_workflows(self.mock_workflow_metas)

        assert isinstance(test_iterator, map)

        expect_result = ['fake-id-1', 'fake-id-2', 'fake-id-3']
        for idx, item in enumerate(test_iterator):
            assert item.id == expect_result[idx]

    @pytest.mark.timeout(2)
    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_fail_with_500,
           create=True)
    def test_execution_event_goes_back_to_sleep_directly_when_it_fails_to_retrieve_workflows(self, caplog):
        """
        This function asserts when the `queue_handler.execution_event()` fails to retrieve any workflow, it will go
        back to sleep directly.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler(self.config_path)
        test_handler.queue_update_interval = 1

        start = timeit.default_timer()
        test_handler.execution_event()
        stop = timeit.default_timer()
        elapsed = stop - start

        info = caplog.text

        assert 'is warmed up and running.' in info
        assert 'Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt.' in info
        assert test_handler.queue_update_interval <= elapsed <= test_handler.queue_update_interval * 1.5
