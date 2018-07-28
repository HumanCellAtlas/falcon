import logging
import timeit
from queue import Queue
from unittest import mock
from unittest.mock import patch

import pytest

from falcon.test import cromwell_simulator
from falcon import igniter
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


def mock_workflow_generator():
    """
    This function generates a mocked `queue_handler.Workflow` object, it has the right type, fake (workflow) `id` and
    fake `bundle_uuid`; it also implements the exact same `__repr__()` as `Workflow` object does.
    """
    mock_workflow = mock.MagicMock(spec=queue_handler.Workflow)
    mock_workflow.id = 'fake_workflow_id'
    mock_workflow.bundle_uuid = 'fake_bundle_uuid'
    mock_workflow.__repr__ = mock.Mock()
    mock_workflow.__repr__.return_value = mock_workflow.id
    return mock_workflow


@mock.create_autospec
def mock_igniter_execution(self, handler):
    """
    This function mocks the `igniter.execution()` instance method, it doesn't have any functionality except checking
    the parameter `handler` has the type `queue_handler.Queue_Handler`. The motivation of mocking this is to avoid
    executing the actual while loop in `igniter.execution()` during the unittest.
    """
    assert isinstance(handler, queue_handler.Queue_Handler)
    return True


class TestIgniter(object):
    """
    This class hosts all unittest cases for testing the `igniter.Igniter` and its methods.
    """

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_igniter_cannot_spawn_and_start_without_a_queue_handler_object(self):
        """
        This function asserts the `igniter.spawn_and_start()` can only accept a valid `queue_handler.Queue_Handler`
        object, otherwise it will throws a `TypeError`.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `Igniter` succeeds.

        Testing Logic: pass an object which is not an instance of `queue_handler.Queue_Handler` into the
        `spawn_and_start()`, expect a `TypeError`.
        """
        not_a_real_queue_handler = type('fake_handler', (object,), {'method': lambda self: print('')})()

        with pytest.raises(TypeError):
            test_igniter = igniter.Igniter('mock_path')
            test_igniter.spawn_and_start(not_a_real_queue_handler)

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch.object(igniter.Igniter, 'execution', new=mock_igniter_execution)
    def test_igniter_can_spawn_and_start_properly_with_a_queue_handler_object(self):
        """
        This function asserts the `igniter.spawn_and_start()` can be executed properly.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `Igniter` succeeds.

        The `@patch.object` here mocks the `igniter.execution()` instance mehtod with `mock_igniter_execution()` to
        avoid executing the actual while loop in `igniter.execution()` during the unittest.

        Testing Logic: pass a mocked instance of `queue_handler.Queue_Handler` into the `spawn_and_start()`, expect
        the `mock_igniter_execution()` to be called once with the mocked instance.
        """
        mock_handler = mock.MagicMock(spec=queue_handler.Queue_Handler)
        mock_queue = Queue(maxsize=1)
        mock_handler.workflow_queue = mock_queue

        test_igniter = igniter.Igniter('mock_path')
        try:
            test_igniter.spawn_and_start(mock_handler)
            mock_igniter_execution.assert_called_once_with(test_igniter, mock_handler)
        finally:
            test_igniter.thread.join()

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        """
        This function asserts the `igniter.sleep_for()` pauses the thread for at least a given duration.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `Igniter` succeeds.

        Testing Logic: instantiate a `Igniter`, defines a const sleep time `test_sleep_time` and pass it into the
        `igniter.sleep_for()` and count the execution time. Expect `test_sleep_time <= elapsed <= test_sleep_time * 1.5`
        which means the `igniter.sleep_for()` can sleep for at least `test_sleep_time` and will wake up no later than
        `test_sleep_time * 1.5`.
        """
        test_igniter = igniter.Igniter('mock_path')
        test_sleep_time = 1

        start = timeit.default_timer()
        test_igniter.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_igniter_join_can_handle_exception(self, caplog):
        """
        This function asserts the `igniter.join()` handles the exception properly, meanwhile, insufficiently, this
        to some extent, tests the availability of `igniter.join()`, since it's just a wrapper around the
        `threading.Thread.join()`.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `Igniter` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a inactive thread within `test_igniter`, and call `igniter.join()`, expect a specific
        logging error appears to the logging stream.
        """
        caplog.set_level(logging.ERROR)
        test_igniter = igniter.Igniter('mock_path')

        assert test_igniter.thread is None

        test_igniter.join()
        error = caplog.text

        assert 'The thread of this igniter is not in a running state.' in error

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_succeed, create=True)
    def test_start_workflow_successfully_releases_a_workflow_and_sleeps_properly(self, caplog):
        """
        This function asserts the `igniter.start_workflow()` can work properly when it gets 200 OK from the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `Igniter` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.release_workflow()` with the
        `cromwell_simulator.release_workflow_succeed`, so that we can test the igniter without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a mocked `queue_handler.Workflow` object and put it into a mocked `queue.Queue` object.
        Create a testing igniter object, defines a short `workflow_start_interval`, say 1 sec here. Call the
        `igniter.start_workflow()` with this mocked queue and make sure it can get 200 OK by using monkey-patched
        `cromwell_tools.release_workflow()`. Expect a specific logging info appears to the logging stream and also
        expect `test_sleep_time <= elapsed <= test_sleep_time * 1.5`.
        """
        caplog.set_level(logging.INFO)
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = igniter.Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        info = caplog.text

        assert mock_queue.empty() is True
        assert 'Ignited a workflow fake_workflow_id' in info
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_403, create=True)
    def test_start_workflow_dose_not_sleep_for_403_response_code(self, caplog):
        """
        This function asserts the `igniter.start_workflow()` can work properly when it gets 403 error from the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `Igniter` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.release_workflow()` with the
        `cromwell_simulator.release_workflow_succeed`, so that we can test the igniter without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a mocked `queue_handler.Workflow` object and put it into a mocked `queue.Queue` object.
        Create a testing igniter object, defines a short `workflow_start_interval`, say 1 sec here. Call the
        `igniter.start_workflow()` with this mocked queue and make sure it can get 403 error by using monkey-patched
        `cromwell_tools.release_workflow()`. Expect some specific logging warnings appears to the logging stream and
        also expect `elapsed < test_igniter.workflow_start_interval` which proves it won't go to sleep when 403 occurs.
        """
        caplog.set_level(logging.WARNING)
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = igniter.Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert 'Skip sleeping to avoid idle time' in warn
        assert elapsed < test_igniter.workflow_start_interval


    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_404, create=True)
    def test_start_workflow_sleeps_properly_for_404_response_code(self, caplog):
        """
        This function asserts the `igniter.start_workflow()` can work properly when it gets 404 error from the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `Igniter` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.release_workflow()` with the
        `cromwell_simulator.release_workflow_succeed`, so that we can test the igniter without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a mocked `queue_handler.Workflow` object and put it into a mocked `queue.Queue` object.
        Create a testing igniter object, defines a short `workflow_start_interval`, say 1 sec here. Call the
        `igniter.start_workflow()` with this mocked queue and make sure it can get 404 error by using monkey-patched
        `cromwell_tools.release_workflow()`. Expect some specific logging warnings appears to the logging stream and
        also expect `test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5`
        which proves it goes back to sleep.
        """
        caplog.set_level(logging.WARNING)
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = igniter.Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert 'Skip sleeping to avoid idle time' not in warn
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_500, create=True)
    def test_start_workflow_sleeps_properly_for_500_response_code(self, caplog):
        """
        This function asserts the `igniter.start_workflow()` can work properly when it gets 500 error from the Cromwell.

        The first `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the
        instantiation of `Igniter` succeeds.

        The second `@patch` here monkey patches the `cromwell_tools.release_workflow()` with the
        `cromwell_simulator.release_workflow_succeed`, so that we can test the igniter without actually talking to
        the Cromwell API.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create a mocked `queue_handler.Workflow` object and put it into a mocked `queue.Queue` object.
        Create a testing igniter object, defines a short `workflow_start_interval`, say 1 sec here. Call the
        `igniter.start_workflow()` with this mocked queue and make sure it can get 500 error by using monkey-patched
        `cromwell_tools.release_workflow()`. Expect some specific logging warnings appears to the logging stream and
        also expect `test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5`
        which proves it goes back to sleep.
        """
        caplog.set_level(logging.WARNING)
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = igniter.Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert 'Skip sleeping to avoid idle time' not in warn
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_start_workflow_sleeps_properly_for_empty_queue(self, caplog):
        """
        This function asserts the `igniter.start_workflow()` goes back to sleep when there is no available entry to be
        processed.

        The `@patch` here mocks the `settings.get_settings()` with `mock_get_settings()` to make sure the instantiation
        of `Igniter` succeeds.

        `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

        Testing Logic: create an empty `queue.Queue` and call with `igniter.start_workflow()`, expect a specific
        logging info appears to the logging stream and also expect
        `test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5` which means
        it goes back to sleep to give some time to queue handler to prepare the next available entry.
        """
        caplog.set_level(logging.INFO)
        mock_queue = Queue(maxsize=1)
        assert mock_queue.empty() is True

        test_igniter = igniter.Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        info = caplog.text

        assert 'The in-memory queue is empty, waiting for the handler to retrieve workflows.' in info
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5
