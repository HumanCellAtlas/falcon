import logging
import os
import timeit
from requests.exceptions import ConnectionError, HTTPError
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
        workflow_id='fake_workflow_id',
        bundle_uuid='fake_bundle_uuid',
        bundle_version='fake_bundle_version',
    )

    def test_igniter_cannot_spawn_and_start_without_having_a_reference_to_a_queue_handler_object(
        self
    ):
        """
        This function asserts the `igniter.spawn_and_start()` can only run by accepting a valid
        `queue_handler.QueueHandler` object, otherwise it will throws a `TypeError`.
        """
        not_a_real_queue_handler = type(
            'fake_handler', (object,), {'method': lambda self: print('')}
        )()

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
            mock_igniter_execution_loop.assert_called_once_with(
                test_igniter, mock_handler
            )
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

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_succeed,
        create=True,
    )
    def test_release_workflow_successfully_releases_a_workflow(self, caplog):
        """
        This function asserts the `igniter.release_workflow()` can work properly when it gets 200 OK from the Cromwell.
        """
        caplog.set_level(logging.INFO)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.release_workflow(self.mock_workflow)

        info = caplog.text

        assert 'Released a workflow fake_workflow_id' in info

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_with_403,
        create=True,
    )
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

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_with_404,
        create=True,
    )
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

    @patch(
        'falcon.igniter.CromwellAPI.release_hold',
        cromwell_simulator.release_workflow_with_500,
        create=True,
    )
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

    def setup_queue_handler(self, workflow=None):
        mock_queue = Queue(maxsize=1)
        if workflow is not None:
            mock_queue.get = mock.Mock(return_value=workflow)
        mock_handler = mock.MagicMock(spec=queue_handler.QueueHandler)
        mock_handler.workflow_queue = mock_queue
        return mock_handler

    def test_execution_event_sleeps_properly_for_empty_queue(self, caplog):
        """
        This function asserts the `igniter.execution_event()` goes back to sleep when there is no available entry in the
        queue to be processed.
        """
        caplog.set_level(logging.INFO)
        mock_handler = self.setup_queue_handler()
        assert mock_handler.workflow_queue.empty() is True

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.execution_event(mock_handler)
        stop = timeit.default_timer()
        elapsed = stop - start

        info = caplog.text

        assert (
            'The in-memory queue is empty, go back to sleep and wait for the handler to retrieve workflows.'
            in info
        )
        assert (
            test_igniter.workflow_start_interval
            <= elapsed
            <= test_igniter.workflow_start_interval * 1.5
        )

    def execution_event_with_mocks(
        self,
        workflow,
        release_calls,
        abort_calls,
        is_dupe_return=None,
        is_dupe_effect=None,
    ):
        mock_handler = self.setup_queue_handler(workflow=workflow)

        test_igniter = igniter.Igniter(self.config_path)
        test_igniter.workflow_is_duplicate = mock.Mock(
            return_value=is_dupe_return, side_effect=is_dupe_effect
        )
        test_igniter.release_workflow = mock.Mock()
        test_igniter.abort_workflow = mock.Mock()

        test_igniter.execution_event(mock_handler)
        assert test_igniter.release_workflow.call_count is release_calls
        assert test_igniter.abort_workflow.call_count is abort_calls

    def test_execution_event_aborts_duplicate_workflow(self, caplog):
        """
        This function asserts the `igniter.execution_event()` aborts a workflow if there
        are existing workflows in cromwell with the same hash-id  (regardless of status).
        """
        caplog.set_level(logging.INFO)
        self.execution_event_with_mocks(
            workflow=queue_handler.Workflow('fake_workflow_id'),
            release_calls=0,
            abort_calls=1,
            is_dupe_return=True,
        )

    def test_execution_event_releases_duplicate_workflow_with_force(self, caplog):
        """
        This function asserts the `igniter.execution_event()` releases a workflow if it contains
        the label 'force' even if there are existing workflows in cromwell with the same
        key-data hash.
        """
        caplog.set_level(logging.INFO)
        self.execution_event_with_mocks(
            workflow=queue_handler.Workflow('fake_workflow_id', labels={'force': None}),
            release_calls=1,
            abort_calls=0,
            is_dupe_return=True,
        )

    def test_execution_event_releases_non_duplicate_workflow(self, caplog):
        """
        This function asserts the `igniter.execution_event()` releases a workflow if there
        are no existing workflows in cromwell with the same hash-id.
        """
        caplog.set_level(logging.INFO)
        self.execution_event_with_mocks(
            workflow=queue_handler.Workflow('fake_workflow_id'),
            release_calls=1,
            abort_calls=0,
            is_dupe_return=False,
        )

    def test_execution_event_does_nothing_on_query_failure(self, caplog):
        """
        This function asserts the `igniter.execution_event()` goes back to sleep when it fails when
        checking if there are existing workflows in cromwell with the same hash-id.
        """
        caplog.set_level(logging.INFO)
        self.execution_event_with_mocks(
            workflow=queue_handler.Workflow('fake_workflow_id'),
            release_calls=0,
            abort_calls=0,
            is_dupe_effect=mock.Mock(side_effect=ConnectionError()),
        )

    def test_execution_event_does_nothing_when_query_status_not_200(self, caplog):
        """
        This function assers the `igniter.execution_event()` goes back to sleep when it
        receives a non 200 response when checking if there are existing workflows in cromwell
        with the same hash-id
        """
        caplog.set_level(logging.INFO)
        self.execution_event_with_mocks(
            workflow=queue_handler.Workflow('fake_workflow_id'),
            release_calls=0,
            abort_calls=0,
            is_dupe_effect=mock.Mock(side_effect=HTTPError()),
        )

    @patch(
        'falcon.igniter.CromwellAPI.query',
        cromwell_simulator.query_workflows_succeed,
        create=True,
    )
    def test_workflow_is_duplicate_returns_true_when_it_finds_workflow_with_same_hash_id(
        self, caplog
    ):
        caplog.set_level(logging.INFO)
        test_igniter = igniter.Igniter(self.config_path)
        assert (
            test_igniter.workflow_is_duplicate(
                workflow=queue_handler.Workflow(
                    'fake_workflow_id', labels={'hash-id': ''}
                )
            )
            is True
        )

    @patch(
        'falcon.igniter.CromwellAPI.query',
        cromwell_simulator.query_workflows_return_fake_workflow,
        create=True,
    )
    def test_workflow_is_duplicate_returns_false_when_it_only_finds_input_workflow(
        self, caplog
    ):
        caplog.set_level(logging.INFO)
        test_igniter = igniter.Igniter(self.config_path)
        assert (
            test_igniter.workflow_is_duplicate(
                workflow=queue_handler.Workflow(
                    'fake_workflow_id', labels={'hash-id': ''}
                )
            )
            is False
        )

    @patch(
        'falcon.igniter.CromwellAPI.query',
        cromwell_simulator.query_workflows_returns_on_hold_workflows_with_duplicate_bundle_versions,
        create=True,
    )
    def test_workflow_is_duplicate_handles_duplicate_bundle_version_in_queue(
            self, caplog
    ):
        caplog.set_level(logging.INFO)
        test_igniter = igniter.Igniter(self.config_path)
        assert (
            test_igniter.workflow_is_duplicate(
                workflow=queue_handler.Workflow(
                    'fake_workflow_id_1',
                    bundle_version='2019-08-22T120000.000000Z',
                    labels={'hash-id': '12345'}
                )
            )
            is False
        )

    @patch(
        'falcon.igniter.CromwellAPI.query',
        cromwell_simulator.query_workflows_returns_workflows_with_different_bundle_versions,
        create=True,
    )
    def test_workflow_is_duplicate_returns_true_if_newer_bundle_version_is_on_hold(
            self, caplog
    ):
        caplog.set_level(logging.INFO)
        test_igniter = igniter.Igniter(self.config_path)
        assert (
            test_igniter.workflow_is_duplicate(
                workflow=queue_handler.Workflow(
                    'fake_workflow_id_1',
                    bundle_version='2019-08-22T120000.000000Z',
                    labels={'hash-id': '12345'}
                )
            )
            is True
        )


    @patch(
        'falcon.igniter.CromwellAPI.query',
        cromwell_simulator.query_workflows_returns_workflows_with_different_bundle_versions,
        create=True,
    )
    def test_workflow_is_duplicate_returns_false_if_bundle_is_the_latest_version_on_hold(
            self, caplog
    ):
        caplog.set_level(logging.INFO)
        test_igniter = igniter.Igniter(self.config_path)
        assert (
            test_igniter.workflow_is_duplicate(
                workflow=queue_handler.Workflow(
                    'fake_workflow_id_2',
                    bundle_version='2019-08-22T130000.000000Z',
                    labels={'hash-id': '12345'}
                )
            )
            is False
        )
