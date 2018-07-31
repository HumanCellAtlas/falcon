import logging
import timeit
from unittest import mock
from unittest.mock import patch
from queue import Queue
import queue

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
    This function mocks the `queue_handler.execution()` instance method, it doesn't have any functionality.
    The motivation of mocking this is to avoid executing the actual while loop in `queue_handler.execution()`
     during the unittest.
    """
    return True


def mock_queue_handler_retrieve_workflows(self, query_dict):
    """
    This function mocks the `queue_handler.QueueHandler.retrieve_workflows()` in successful situations,
    it always returns a fixed list of workflow query results as for testing purposes.
    """
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
    """
    This function mocks the `queue_handler.QueueHandler.retrieve_workflows()` in failed situations,
    it always returns an empty list as the query result for testing purposes.
    """
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

        Note: In the future, if we want to add advanced de-duplication feature to the service, besides asserting
        workflow id between 2 Workflow objects, we might also want to check if they have the same bundle_uuid and
        bundle_version.
        """
        test_workflow1 = queue_handler.Workflow(
            workflow_id='fake-workflow-1', bundle_uuid='fake-bundle-uuid-1')

        test_workflow2 = queue_handler.Workflow(
            workflow_id='fake-workflow-2', bundle_uuid='fake-bundle-uuid-1')

        assert test_workflow1 != test_workflow2


class QueueHandler(object):
    """
    This class hosts all unittest cases for testing the `queue_handler.QueueHandler` and its methods.
    """

    def test_create_empty_queue_returns_a_valid_empty_queue_object(self):
        """
        This function asserts the `queue_handler.create_empty_queue()` returns a valid `queue.Queue` object and it is empty.
        """
        q = queue_handler.QueueHandler.create_empty_queue()
        assert isinstance(q, Queue)
        assert q.empty() is True

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch.object(queue_handler.QueueHandler, 'execution', new=mock_queue_handler_execution)
    def test_queue_handler_can_spawn_and_start_properly(self):
        """
        This function asserts the `queue_handler.spawn_and_start()` can be executed properly.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The `@patch.object` here mocks the `queue_handler.execution()` instance method with
        `mock_queue_handler_execution()` to avoid executing the actual while loop in `queue_handler.execution()`
        during the unittest.

        Testing Logic: pass a mocked instance of `queue_handler.QueueHandler` into the `spawn_and_start()`, expect
        the `mock_queue_handler_execution()` to be called once with the mocked instance.
        """
        test_handler = queue_handler.QueueHandler('mock_path')
        try:
            test_handler.spawn_and_start()
            mock_queue_handler_execution.assert_called_once_with(test_handler)
        finally:
            test_handler.thread.join()

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        """
        This function asserts the `queue_handler.sleep_for()` pauses the thread for at least a given duration.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `QueueHandler` succeeds.

        Testing Logic: instantiate a `QueueHandler`, defines a const sleep time `test_sleep_time` and pass it into the
        `queue_handler.sleep_for()` and count the execution time. Expect
        `test_sleep_time <= elapsed <= test_sleep_time * 1.5` which means the `queue_handler.sleep_for()` can sleep
        for at least `test_sleep_time` and will wake up no later than `test_sleep_time * 1.5`.
        """
        test_handler = queue_handler.QueueHandler('mock_path')
        test_sleep_time = 1

        start = timeit.default_timer()
        test_handler.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_queue_handler_join_can_handle_exception(self, caplog):
        """
        This function asserts the `queue_handler.join()` handles the exception properly, meanwhile, insufficiently, this
        to some extent, tests the availability of `queue_handler.join()`, since it's just a wrapper around the
        `threading.Thread.join()`.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `QueueHandler` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create an inactive thread within `test_handler`, and call `queue_handler.join()`, expect a
        specific logging error appears to the logging stream.
        """
        caplog.set_level(logging.ERROR)
        test_handler = queue_handler.QueueHandler('mock_path')

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

        assert queue_handler.QueueHandler.is_workflow_list_in_oldest_first_order(
            newest_first_workflow_list
        ) is False

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_succeed, create=True)
    def test_retrieve_workflows_returns_query_results_successfully(self):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly when it gets 200 OK from
        the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.query_workflows()` with the
        `cromwell_simulator.query_workflows_succeed`, so that we can test the handler without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, call the `queue_handler.retrieve_workflows()` and
        make sure it can get 200 OK by using monkey-patched `cromwell_tools.query_workflows()`. Expect the returned
        result is a list and it's not empty.
        """
        test_handler = queue_handler.QueueHandler('mock_path')
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        assert isinstance(results, list)
        assert len(results) > 0

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.cromwell_tools.query_workflows', cromwell_simulator.query_workflows_fail_with_500,
           create=True)
    def test_retrieve_workflows_returns_empty_list_on_exceptions(self, caplog):
        """
        This function asserts the `queue_handler.retrieve_workflows()` works properly when it gets error codes from
        the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.query_workflows()` with the
        `cromwell_simulator.query_workflows_fail_with_500`, so that we can test the handler without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, call the `queue_handler.retrieve_workflows()`, make sure
        it can get a 500 error code by using monkey-patched `cromwell_tools.query_workflows()`. Expect the returned
        result is a list and it's actually empty, also expect to see a specific logging warning appears to the logging
        stream.
        """
        caplog.set_level(logging.WARNING)
        test_handler = queue_handler.QueueHandler('mock_path')
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        warn = caplog.text

        assert isinstance(results, list)
        assert len(results) == 0
        assert 'Failed to retrieve workflows from Cromwell' in warn

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_indeed_rebuilds_a_new_workflow_queue_and_changes_the_reference_pointer_properly(self, caplog):
        """
        This function asserts the `queue_handler.enqueue()` rebuilds a new `queue.Queue` object and points the reference
        to the queue when it retrieves at least one workflow from the cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, note down the identity of the queue_object at this moment
        by calling the python built-in `id()` function (CPython implementation detail: This is the address of the
        object in memory.) By calling the mocked `retrieve_workflows()` we get a fixed result, which is actually a
        list of three pre-defined workflow metadata blocks. Pass the list into the `enqueue()` function and check the
        identity of the `workflow_queue` object again, expect to see the id has been changed, which means a new Queue
        object has been created and replaced the old one in the memory. Also expect a specific logging info appears to
        the logging stream.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler('mock_path')
        initial_queue_id = id(test_handler.workflow_queue)
        mock_results = mock_queue_handler_retrieve_workflows(test_handler, test_handler.cromwell_query_dict)
        mock_counts = len(mock_results)
        assert mock_counts != 0

        test_handler.enqueue(mock_results)

        final_queue_id = id(test_handler.workflow_queue)

        info = caplog.text

        assert 'Retrieved {0} workflows from Cromwell.'.format(mock_counts) in info
        assert initial_queue_id != final_queue_id

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_can_put_workflow_objects_to_the_new_workflow_queue(self, caplog):
        """
        This function asserts the `queue_handler.enqueue()` put all retrieved workflows to its `workflow_queue` variable
        when it retrieves at least one workflow from the cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, by calling the mocked `retrieve_workflows()` we get a
        fixed result, which is actually a list of three pre-defined workflow metadata blocks. Pass the list into the
        `enqueue()` function and expect a specific logging info appears to the logging stream. Besides, call
        `Queue.get()` 3 times to check if the objects in the queue are actually the `Workflow` objects we want.
        """
        caplog.set_level(logging.DEBUG)
        test_handler = queue_handler.QueueHandler('mock_path')
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

        assert isinstance(wf1, queue_handler.Workflow) and wf1.id == 'fake-id-1'
        assert isinstance(wf2, queue_handler.Workflow) and wf2.id == 'fake-id-2'
        assert isinstance(wf3, queue_handler.Workflow) and wf3.id == 'fake-id-3'

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_enqueue_goes_back_to_sleep_when_no_workflow_is_retrieved(self, caplog):
        """
        This function asserts the `queue_handler.enqueue()` doesn't rebuild a new `queue.Queue` object and skips
        enqueuing when it retrieves no workflow from the cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, note down the identity of the queue_object at this moment
        by calling the python built-in `id()` function (CPython implementation detail: This is the address of the
        object in memory.) By calling the mocked `retrieve_workflows()` we get a fixed result, which is actually a
        list of three pre-defined workflow metadata blocks. Pass the list into the `enqueue()` function and check the
        identity of the `workflow_queue` object again, expect to see the id is not been changed, which means the Queue
        object remains in the memory. Also expect a specific logging info appears to the logging stream.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler('mock_path')
        initial_queue_id = id(test_handler.workflow_queue)
        mock_results = mock_queue_handler_retrieve_no_workflow(test_handler, test_handler.cromwell_query_dict)

        test_handler.enqueue(mock_results)

        final_queue_id = id(test_handler.workflow_queue)

        info = caplog.text

        assert 'Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt.' in info
        assert initial_queue_id == final_queue_id
