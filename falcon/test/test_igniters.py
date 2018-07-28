import logging
import timeit
from queue import Queue
from unittest import mock
from unittest.mock import patch

import pytest

from falcon.igniter import Igniter
from . import cromwell_simulator


def mock_get_settings(path):
    return {
        'cromwell_url': 'https://example.cromwell-instance.org/api/workflows/v1',
        'use_caas': False,
        'cromwell_user': 'username',
        'cromwell_password': 'password',
        'queue_update_interval': 60,
        'workflow_start_interval': 10
    }


def mock_workflow_generator():
    mock_workflow = mock.MagicMock()
    mock_workflow.id = 'fake_workflow_id'
    mock_workflow.bundle_uuid = 'fake_bundle_uuid'
    mock_workflow.__repr__ = mock.Mock()
    mock_workflow.__repr__.return_value = mock_workflow.id
    return mock_workflow


@mock.create_autospec
def mock_igniter_execution(self, queue):
    assert queue is not None
    return True


class TestIgniter(object):

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_igniter_cannot_spawn_and_start_without_a_queue_object(self):
        not_a_real_queue = []

        with pytest.raises(TypeError):
            test_igniter = Igniter('mock_path')
            test_igniter.spawn_and_start(not_a_real_queue)

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch.object(Igniter, 'execution', new=mock_igniter_execution)
    def test_igniter_can_spawn_and_start_properly_with_a_queue_object(self):
        mock_queue = Queue(maxsize=1)
        test_igniter = Igniter('mock_path')
        try:
            test_igniter.spawn_and_start(mock_queue)
            mock_igniter_execution.assert_called_once_with(test_igniter, mock_queue)
        finally:
            test_igniter.thread.join()

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_sleep_for_can_pause_for_at_least_given_duration(self):
        test_igniter = Igniter('mock_path')
        test_sleep_time = 1

        start = timeit.default_timer()
        test_igniter.sleep_for(test_sleep_time)
        stop = timeit.default_timer()
        elapsed = stop - start

        assert test_sleep_time <= elapsed <= test_sleep_time * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_igniter_join_can_handle_exception(self, caplog):
        test_igniter = Igniter('mock_path')

        assert test_igniter.thread is None

        caplog.set_level(logging.ERROR)
        test_igniter.join()
        error = caplog.text

        assert 'The thread of this igniter is not in a running state.' in error

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_succeed, create=True)
    def test_start_workflow_successfully_releases_a_workflow_and_sleeps_properly(self, caplog):
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        caplog.set_level(logging.INFO)
        info = caplog.text

        assert mock_queue.empty() is True
        assert 'Ignited a workflow fake_workflow_id' in info
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_403, create=True)
    def test_start_workflow_dose_not_sleep_for_403_response_code(self, caplog):
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        caplog.set_level(logging.WARNING)
        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert 'Skip sleeping to avoid idle time' in warn
        assert elapsed < test_igniter.workflow_start_interval


    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_404, create=True)
    def test_start_workflow_sleeps_properly_for_404_response_code(self, caplog):
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        caplog.set_level(logging.WARNING)
        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    @patch('falcon.igniter.cromwell_tools.release_workflow', cromwell_simulator.release_workflow_with_500, create=True)
    def test_start_workflow_sleeps_properly_for_500_response_code(self, caplog):
        mock_workflow = mock_workflow_generator()

        mock_queue = Queue(maxsize=1)
        mock_queue.put(mock_workflow)
        assert mock_queue.empty() is False

        test_igniter = Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        caplog.set_level(logging.WARNING)
        warn = caplog.text

        assert mock_queue.empty() is True
        assert 'Failed to start a workflow fake_workflow_id' in warn
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5

    @patch('falcon.igniter.settings.get_settings', mock_get_settings)
    def test_start_workflow_sleeps_properly_for_empty_queue(self, caplog):
        mock_queue = Queue(maxsize=1)
        assert mock_queue.empty() is True

        test_igniter = Igniter('mock_path')
        test_igniter.workflow_start_interval = 1

        start = timeit.default_timer()
        test_igniter.start_workflow(mock_queue)
        stop = timeit.default_timer()
        elapsed = stop - start

        caplog.set_level(logging.INFO)
        info = caplog.text

        assert 'The in-memory queue is empty, wait for the handler to retrieve workflow before next check-in.' in info
        assert test_igniter.workflow_start_interval <= elapsed <= test_igniter.workflow_start_interval * 1.5
