import logging
import queue
import time
from datetime import datetime
from threading import Thread, get_ident

import requests
from cromwell_tools.cromwell_api import CromwellAPI

from falcon import queue_handler
from falcon import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f'falcon.{__name__}')


class Igniter(object):
    def __init__(self, config_path):
        """Concrete Igniter class for igniting/starting workflows in Cromwell.

        Args:
            config_path (str): Path to the config.json file.
        """
        self.thread = None
        self.settings = settings.get_settings(config_path)
        self.cromwell_auth = settings.get_cromwell_auth(self.settings)
        self.workflow_start_interval = self.settings.get('workflow_start_interval')

    def spawn_and_start(self, handler):
        if not isinstance(handler, queue_handler.QueueHandler):
            raise TypeError(
                'Igniter has to access to an instance of the QueueHandler to start!'
            )

        if not self.thread:
            # TODO: by appending an uuid to the thread name, we can have multiple igniter threads here in the future
            self.thread = Thread(
                target=self.execution_loop, name='igniter', args=(handler,)
            )
        self.thread.start()

    def join(self):
        try:
            self.thread.join()
        except (AttributeError, AssertionError):
            logger.error('The thread of this igniter is not in a running state.')

    def execution_loop(self, handler):
        logger.info(
            f'Igniter | Initializing an igniter with thread ID => {get_ident()} | {datetime.now()}'
        )
        while True:
            self.execution_event(handler)

    def execution_event(self, handler):
        logger.info(
            f'Igniter | Igniter thread {get_ident()} is warmed up and running. | {datetime.now()}'
        )
        try:
            workflow = handler.workflow_queue.get(block=False)
            if 'force' not in workflow.labels.keys() and self.workflow_is_duplicate(
                workflow
            ):
                logger.info(
                    'Igniter | Found existing workflow with the same hash-id; '
                    f'aborting workflow {workflow} | {datetime.now()}'
                )
                self.abort_workflow(workflow)
            else:
                self.release_workflow(workflow)
        except queue.Empty:
            logger.info(
                'Igniter | The in-memory queue is empty, go back to sleep and wait for the handler to retrieve '
                f'workflows. | {datetime.now()}'
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
        ) as error:
            logger.error(
                f'Igniter | Failed to query cromwell for existing workflows {error} | {datetime.now()}'
            )
        finally:
            self.sleep_for(self.workflow_start_interval)

    def simple_cromwell_workflow_action(
        self, do_thing, workflow, failure_message, success_message
    ):
        try:
            response = do_thing(uuid=workflow.id, auth=self.cromwell_auth)
            if response.status_code != 200:
                logger.warning(
                    f'Igniter | {failure_message} {workflow} | {response.text} | {datetime.now()}'
                )
            else:
                logger.info(
                    f'Igniter | {success_message} {workflow} | {datetime.now()}'
                )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
        ) as error:
            logger.error(
                f'Igniter | {failure_message} {workflow} | {error} | {datetime.now()}'
            )

    def release_workflow(self, workflow):
        self.simple_cromwell_workflow_action(
            do_thing=CromwellAPI.release_hold,
            workflow=workflow,
            failure_message='Failed to release a workflow',
            success_message='Released a workflow',
        )

    def abort_workflow(self, workflow):
        self.simple_cromwell_workflow_action(
            do_thing=CromwellAPI.abort,
            workflow=workflow,
            failure_message='Failed to abort a workflow',
            success_message='Aborted a workflow',
        )

    def workflow_is_duplicate(self, workflow):
        hash_id = workflow.labels.get('hash-id')
        query_dict = {'label': f'hash-id:{hash_id}'}
        response = CromwellAPI.query(
            query_dict, self.cromwell_auth, raise_for_status=True
        )
        results = response.json()['results']
        return any([result['id'] != workflow.id for result in results])

    @staticmethod
    def sleep_for(sleep_time):
        time.sleep(sleep_time)
