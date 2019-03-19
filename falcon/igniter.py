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
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


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
            'Igniter | Initializing an igniter with thread ID => {0} | {1}'.format(
                get_ident(), datetime.now()
            )
        )
        while True:
            self.execution_event(handler)

    def execution_event(self, handler):
        logger.info(
            'Igniter | Igniter thread {0} is warmed up and running. | {1}'.format(
                get_ident(), datetime.now()
            )
        )
        try:
            workflow = handler.workflow_queue.get(block=False)
            self.release_workflow(workflow)
        except queue.Empty:
            logger.info(
                'Igniter | The in-memory queue is empty, go back to sleep and wait for the handler to retrieve '
                'workflows. | {0}'.format(datetime.now())
            )
        finally:
            self.sleep_for(self.workflow_start_interval)

    def release_workflow(self, workflow):
        try:
            response = CromwellAPI.release_hold(
                uuid=workflow.id, auth=self.cromwell_auth
            )
            if response.status_code != 200:
                logger.warning(
                    'Igniter | Failed to release a workflow {0} | {1} | {2}'.format(
                        workflow, response.text, datetime.now()
                    )
                )
            else:
                logger.info(
                    'Igniter | Released a workflow {0} | {1}'.format(
                        workflow, datetime.now()
                    )
                )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
        ) as error:
            logger.error(
                'Igniter | Failed to release a workflow {0}| {1} | {2}'.format(
                    workflow, error, datetime.now()
                )
            )

    @staticmethod
    def sleep_for(sleep_time):
        time.sleep(sleep_time)
