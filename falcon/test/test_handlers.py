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
    print('Called retrieve_workflows.')
    return results


def mock_queue_handler_retrieve_no_workflow(self, query_dict):
    """
    This function mocks the `queue_handler.QueueHandler.retrieve_workflows()` in the situation which
    it retrieves 0 workflow. This will always return an empty list as the query result for testing purposes.
    """
    results = []
    print('Called retrieve_workflows.')
    return results

def mock_queue_handler_fail_to_retrieve_workflow(self, query_dict):
    """
    This function mocks the `queue_handler.QueueHandler.retrieve_workflows()` in failed situations,
    it always returns None for testing purposes.
    """
    print('Called retrieve_workflows.')
    return None


def mock_sleep_for(self, sleeptime):
    """
    This function mocks the `sleep_for()` static method. It will print a specific message to the stdout and
    raise a `StopIteration` to try to break the infinite loop within `queue_handler.execution()` during the unittest.
    """
    print('Called sleep_for.')
    raise StopIteration


def mock_enqueue(self, workflows):
    """
    This function mocks the `queue_handler.enqueue()` instance method. It will print a specific message to the stdout and
    raise a `StopIteration` to try to break the infinite loop within `queue_handler.execution()` during the unittest.
    """
    for workflow in workflows:
        pass
    print('Called enqueue.')
    raise StopIteration


def mock_set_queue(self, workflows):
    """
    This function mocks the `queue_handler.set_queue()` instance method. It will print a specific message to the stdout
    during the unittest.
    """
    print('Called set_queue.')


def mock_assemble_workflow(self, workflow_meta):
    """
    This function mocks out the actual `queue_handler._assemble_workflow()` function. It will just return the value of
    the `id` field of the input workflow_meta dictionary to make it easier to test.
    """
    return workflow_meta.get('id')


def mock_prepare_workflows(self, workflow_metas):
    """
    This function mocks the `queue_handler.prepare_workflows()` instance method. It will print a specific message to
    the stdout and return an iterator that contains 3 pre-defined `Workflow` instances during the unittest.
    """
    assert len(workflow_metas) == 3
    workflow1 = queue_handler.Workflow(workflow_id='fake-id-1',
                                       bundle_uuid='fake-bundle-uuid-1',
                                       bundle_version='2018-01-01T22:49:40.620Z')

    workflow2 = queue_handler.Workflow(workflow_id='fake-id-2',
                                       bundle_uuid='fake-bundle-uuid-2',
                                       bundle_version='2018-01-01T22:49:40.620Z')

    workflow3 = queue_handler.Workflow(workflow_id='fake-id-3',
                                       bundle_uuid='fake-bundle-uuid-3',
                                       bundle_version='2018-01-01T22:49:40.620Z')

    print('Called prepare_workflows')
    return iter([workflow1, workflow2, workflow3])

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
    def test_retrieve_workflows_returns_none_on_exceptions(self, caplog):
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
        result is None, also expect to see a specific logging warning appears to the logging
        stream.
        """
        caplog.set_level(logging.WARNING)
        test_handler = queue_handler.QueueHandler('mock_path')
        results = test_handler.retrieve_workflows(test_handler.cromwell_query_dict)

        warn = caplog.text

        assert results is None
        assert 'Failed to retrieve workflows from Cromwell' in warn

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.QueueHandler._assemble_workflow', mock_assemble_workflow)
    def test_prepare_workflows_returns_a_workflow_iterator_correctly(self):
        """
        This function asserts the `queue_handler.prepare_workflows()` returns an expected iterator of the list of
        `Workflow` objects.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The second `@patch` here mocks the `queue_handler._assemble_workflow()` to avoid actually assembling the
        `Workflow` instances during the testing.

        Testing Logic: create a testing queue_handler object, call the `prepare_workflows()` function with other
        mocked helper functions. Expect a object with the type `map` is returned, also expect all items in the iterator
        are exactly the workflows.
        """
        test_handler = queue_handler.QueueHandler('mock_path')
        mock_metas = mock_queue_handler_retrieve_workflows(test_handler, test_handler.cromwell_query_dict)
        test_iterator = test_handler.prepare_workflows(mock_metas)

        assert isinstance(test_iterator, map)

        expect_result = ['fake-id-1', 'fake-id-2', 'fake-id-3']
        for idx, item in enumerate(test_iterator):
            assert item == expect_result[idx]

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_assemble_workflow_can_work_on_workflow_metadata_properly(self):
        """
        This function asserts the `queue_handler._assemble_workflow()` properly parses an object of workflow metadata
        and assmeble it as a `Workflow` isntance.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.
        """
        test_metadata = {
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
        }
        test_handler = queue_handler.QueueHandler('mock_path')
        workflow = test_handler._assemble_workflow(test_metadata)

        assert isinstance(workflow, queue_handler.Workflow)
        assert workflow.id == 'fake-id-1'
        assert workflow.bundle_uuid == 'fake-bundle-uuid-1'
        assert workflow.bundle_version == '2018-01-01T22:49:40.620Z'

    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    def test_set_queue_indeed_changes_the_reference_pointer_properly(self, caplog):
        """
        This function asserts the `queue_handler.set_queue()` accepts a `queue.Queue` object and points the reference
        to the queue when it gets called.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a testing queue_handler object, note down the identity of the queue_object at this moment
        by calling the python built-in `id()` function (CPython implementation detail: This is the address of the
        object in memory.) By calling the mocked `set_queue()` check the identity of the `workflow_queue` object again,
        expect to see the id has been changed, which means a new Queue object has been created and replaced the old one
        in the memory. Also expect a specific logging info appears to the logging stream.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler('mock_path')
        initial_queue_id = id(test_handler.workflow_queue)

        another_queue = Queue(-1)
        another_queue_id = id(another_queue)
        test_handler.set_queue(another_queue)

        final_queue_id = id(test_handler.workflow_queue)

        assert initial_queue_id != final_queue_id
        assert final_queue_id == another_queue_id

    @pytest.mark.timeout(1)
    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.QueueHandler.retrieve_workflows', mock_queue_handler_retrieve_workflows)
    @patch('falcon.queue_handler.QueueHandler.prepare_workflows', mock_prepare_workflows)
    @patch('falcon.queue_handler.QueueHandler.set_queue', mock_set_queue)
    @patch('falcon.queue_handler.QueueHandler.enqueue', mock_enqueue)
    def test_execution_calls_prepare_workflows_and_set_queue_and_enqueue_properly(self, caplog, capsys):
        """
        This function asserts when the `queue_handler.execution()` successfully retrieve at least workflow, it can
        handle the rest of the operations properly.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The second `@patch` here mocks the `queue_handler.retrieve_workflows()` to make sure it get mocked workflows
        instead of actually talking to Cromwell during the test.

        The third `@patch` here mocks the `queue_handler.prepare_workflows()` to make sure it get mocked `Workflow`
        objects instead of assembling them during the test.

        The fourth `@patch` here mocks the `queue_handler.set_queue()`, to avoid actually change the pointer to the
        queue during testing.

        The fifth `@patch` here mocks the `queue_handler.enqueue()` so it can raises a `StopIteration`
        exception to stop the infinite loop inside of `execution()`.

        To avoid dangerous hanging unit tests, it also used a `@pytest.mark.timeout(1)` here to set the the timeout to
        be 1 second.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        `capsys` is a fixture of provided by Pytest, which captures all stdout and stderr streams during the test.

        Testing Logic: create an empty `QueueHandler` instance, let it call the `execution()` function with mocked
        helper functions, expect specific stdouts and logging messages, which proves those functions are actually
        called during the testing.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler('mock_path')

        with pytest.raises(StopIteration):
            test_handler.execution()

        info = caplog.text
        captured_stdout, _ = capsys.readouterr()

        assert 'Initializing the queue handler with thread' in info
        assert 'Called retrieve_workflows.\n' in captured_stdout
        assert 'Called prepare_workflows\n' in captured_stdout
        assert 'Called set_queue.\n' in captured_stdout
        assert 'Called enqueue.\n' in captured_stdout

    @pytest.mark.timeout(1)
    @patch('falcon.queue_handler.settings.get_settings', mock_get_settings)
    @patch('falcon.queue_handler.QueueHandler.retrieve_workflows', mock_queue_handler_fail_to_retrieve_workflow)
    @patch('falcon.queue_handler.QueueHandler.sleep_for', mock_sleep_for)
    def test_execution_goes_back_to_sleep_directly_when_it_fails_to_retrieve_workflows(self, caplog, capsys):
        """
        This function asserts when the `queue_handler.execution()` fails to retrieve any workflow, it will go back to
        sleep directly.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `QueueHandler` succeeds.

        The second `@patch` here mocks the `queue_handler.retrieve_workflows()` to make sure it get mocked workflows
        instead of actually talking to Cromwell during the test.

        The second `@patch` here mocks the `queue_handler.sleep_for()` so it can raises a `StopIteration`
        exception to stop the infinite loop inside of `execution()`.

        To avoid dangerous hanging unit tests, it also used a `@pytest.mark.timeout(1)` here to set the the timeout to
        be 1 second.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        `capsys` is a fixture of provided by Pytest, which captures all stdout and stderr streams during the test.

        Testing Logic: create an empty `QueueHandler` instance, let it call the `execution()` function with mocked
        helper functions, expect specific stdouts and logging messages, which proves those functions are actually
        called during the testing.
        """
        caplog.set_level(logging.INFO)
        test_handler = queue_handler.QueueHandler('mock_path')

        with pytest.raises(StopIteration):
            test_handler.execution()

        info = caplog.text
        captured_stdout, _ = capsys.readouterr()

        assert 'Initializing the queue handler with thread' in info
        assert 'Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt.' in info
        assert 'Called retrieve_workflows.\n' in captured_stdout
        assert 'Called sleep_for.\n' in captured_stdout
