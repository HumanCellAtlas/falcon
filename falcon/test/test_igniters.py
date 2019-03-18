import logging
import os
import timeit
from queue import Queue
from unittest import mock
from unittest.mock import patch

import pytest

from falcon import igniter
from falcon import queue_handler
from falcon.test import cromwell_simulator


@mock.create_autospec
def mock_igniter_execution_loop(self, handler):
    """
    This function mocks the `igniter.execution_loop()` instance method, it doesn't have any functionality except
    checking the parameter `handler` has the type `queue_handler.QueueHandler`. The motivation of mocking this is
    to avoid executing the actual `igniter.execution_loop()` during the unittest.
    """
    assert isinstance(handler, queue_handler.QueueHandler)
    return True


class TestIgniter(object):
    """
    This class hosts all unittest cases for testing the `igniter.Igniter` and its methods. This class takes
    advantages of monkey-patch, some mock features and pytest fixtures:

    `caplog` is a fixture of provided by Pytest, which captures all logging streams during the test.

    `@pytest.mark.timeout()` limits the maximum running time for test cases.

    `@patch`s in the class monkey patches the functions that need to talk to an external resource, so that
    we can test the igniter without actually talking to the external resource, in this case, the Cromwell API.
    """

    data_dir = '{}/data/'.format(os.path.split(__file__)[0])
    cromwell_config = 'example_config_cromwell_instance.json'
    config_path = '{0}{1}'.format(data_dir, cromwell_config)
    mock_workflow = queue_handler.Workflow(
        workflow_id='fake_workflow_id', bundle_uuid='fake_bundle_uuid', bundle_version='fake_bundle_version'
    )

    def test_igniter_cannot_spawn_and_start_without_having_a_reference_to_a_queue_handler_object(self):
        """
        This function asserts the `igniter.spawn_and_start()` can only run by accepting a valid
        `queue_handler.QueueHandler` object, otherwise it will throws a `TypeError`.
        """
        not_a_real_queue_handler = type('fake_handler', (object,), {'method': lambda self: print('')})()

        with pytest.raises(TypeError):
            test_igniter = igniter.Igniter(self.config_path)
            test_igniter.spawn_and_start(not_a_real_queue_handler)

    @pytest.mark.timeout(2)
    @patch.object(igniter.Igniter, 'execution_loop', new=mock_igniter_execution_loop)
    def test_igniter_can_spawn_and_start_properly_with_a_queue_handler_object(self):
        """
        This function asserts the `igniter.spawn_and_start()` can be executed properly.
        """
        mock_handler = mock.MagicMock(spec=queue_handler.QueueHandler)
        test_igniter = igniter.Igniter(self.config_path)

        try:
            test_igniter.spawn_and_start(mock_handler)
            mock_igniter_execution_loop.assert_called_once_with(test_igniter, mock_handler)
        finally:
            test_igniter.thread.join()

    @pytest.mark.timeout(2)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        """
        This function asserts the `igniter.sleep_for()` pauses the thread for at least a given duration.
        """
        test_igniter = igniter.Igniter(self.config_path)
        test_sleep_time = 1

        start = timeit.default_timer()
        test_igniter.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    def test_igniter_join_can_handle_exception(self, caplog):
        """
        This function asserts the `igniter.join()` handles the exception properly, meanwhile, insufficiently, this
        to some extent, tests the availability of `igniter.join()`, since it's just a wrapper around the
        `threading.Thread.join()`.
        """
        caplog.set_level(logging.ERROR)
        test_igniter = igniter.Igniter(self.config_path)

        assert test_igniter.thread is None

        test_igniter.join()
        error = caplog.text

        assert 'The thread of this igniter is not in a running state.' in error

    @patch('falcon.igniter.CromwellAPI.release_hold', cromwell_simulator.release_workflow_succeed, create=True)
    def test_release_workflow_successfully_releases_a_workflow(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it gets 200 OK from the Cromwell.
        """
        caplog.set_level(logging.INFO)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        info = caplog.text

        assert 'Released a workflow fake_workflow_id' in info

    @patch('falcon.igniter.CromwellAPI.release_hold', cromwell_simulator.release_workflow_with_403, create=True)
    def test_release_workflow_handles_403_response_code(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it gets 403 error from the
        Cromwell.
        """
        caplog.set_level(logging.WARNING)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        warn = caplog.text

        assert 'Failed to release a workflow fake_workflow_id' in warn

    @patch('falcon.igniter.CromwellAPI.release_hold', cromwell_simulator.release_workflow_with_404, create=True)
    def test_release_workflow_handles_404_response_code(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it gets 404 error from the
        Cromwell.
        """
        caplog.set_level(logging.WARNING)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        warn = caplog.text

        assert 'Failed to release a workflow fake_workflow_id' in warn

    @patch('falcon.igniter.CromwellAPI.release_hold', cromwell_simulator.release_workflow_with_500, create=True)
    def test_release_workflow_handles_500_response_code(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it gets 500 error from the
        Cromwell.
        """
        caplog.set_level(logging.WARNING)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        warn = caplog.text

        assert 'Failed to release a workflow fake_workflow_id' in warn

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_raises_ConnectionError,
        create=True,
    )
    def test_release_workflow_handles_connection_error(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it runs into connection errors
        when talking to Cromwell.
        """
        caplog.set_level(logging.ERROR)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        error = caplog.text

        assert 'Failed to release a workflow fake_workflow_id' in error

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_raises_RequestException,
        create=True,
    )
    def test_release_workflow_handles_requests_exception(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it runs into requests exceptions
        when talking to Cromwell.
        """
        caplog.set_level(logging.ERROR)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        error = caplog.text

        assert 'Failed to release a workflow fake_workflow_id' in error

    def test_execution_event_sleeps_properly_for_empty_queue(self, caplog):
        """
        This function asserts the `igniter.execution_event()` goes back to sleep when there is no available entry in the
        queue to be processed.
        """
        caplog.set_level(logging.INFO)
        mock_queue = Queue(maxsize=1)
        mock_handler = mock.MagicMock(spec=queue_handler.QueueHandler)
        mock_handler.workflow_queue = mock_queue
        assert mock_handler.workflow_queue.empty() is True

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.execution_event(mock_handler)
        stop = timeit.default_timer()
        elapsed = stop - start

        info = caplog.text

        assert 'The in-memory queue is empty, go back to sleep and wait for the handler to retrieve workflows.' in info
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5
