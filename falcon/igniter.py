import logging
import queue
import time
from datetime import datetime
from threading import Thread, get_ident

import requests
from cromwell_tools import cromwell_tools

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
        self.cromwell_url = self.settings.get('cromwell_url')
        self.workflow_start_interval = self.settings.get('workflow_start_interval')

    def spawn_and_start(self, handler):
        if not isinstance(handler, queue_handler.QueueHandler):
            raise TypeError('Igniter has to access to an instance of the QueueHandler to start!')

        if not self.thread:
            self.thread = Thread(target=self.execution_loop, args=(handler,))
        self.thread.start()

    def join(self):
        try:
            self.thread.join()
        except (AttributeError, AssertionError):
            logger.error('The thread of this igniter is not in a running state.')

    def execution_loop(self, handler):
        logger.info('Igniter | Initializing an igniter with thread => {0} | {1}'.format(get_ident(), datetime.now()))
        while True:
            self.execution_event(handler)

    def execution_event(self, handler):
        try:
            workflow = handler.mem_queue.get(block=False)
            self.release_workflow(workflow)
        except queue.Empty:
            logger.info(
                    'Igniter | The in-memory queue is empty, go back to sleep and wait for the handler to retrieve '
                    'workflows. | {0}'.format(
                            datetime.now()))
        finally:
            self.sleep_for(self.workflow_start_interval)

    def release_workflow(self, workflow):
        try:
            response = cromwell_tools.release_workflow(
                    cromwell_url=self.cromwell_url,
                    workflow_id=workflow.id,
                    cromwell_user=self.settings.get('cromwell_user'),
                    cromwell_password=self.settings.get('cromwell_password'),
                    caas_key=self.settings.get('caas_key')
            )
            if response.status_code != 200:
                logger.warning(
                        'Igniter | Failed to release a workflow {0} | {1} | {2}'.format(workflow, response.text,
                                                                                        datetime.now()))
            else:
                logger.info('Igniter | Released a workflow {0} | {1}'.format(workflow, datetime.now()))
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as error:
            logger.error('Igniter | {0} | {1}'.format(error, datetime.now()))

    @staticmethod
    def sleep_for(sleep_time):
        time.sleep(sleep_time)
