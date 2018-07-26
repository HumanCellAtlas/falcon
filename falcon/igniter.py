import logging
from threading import Thread, get_ident
import os
import queue
import time
from datetime import datetime

import cromwell_tools
from settings import get_settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


class Igniter(object):
    def __init__(self, config_path):
        """Concrete Igniter class for igniting/starting workflows in Cromwell.

        Args:
            config_path (str): Path to the config.json file.
        """
        self.thread = None
        self.settings = get_settings(config_path)
        self.cromwell_url = self.settings.get('cromwell_url')
        self.workflow_start_interval = self.settings.get('workflow_start_interval')

    def spawn_and_start(self, mem_queue_from_handler):
        if not isinstance(mem_queue_from_handler, queue.Queue):
            raise TypeError('Igniter has to get a shared Queue object from Queue_Handler to start!')

        if not self.thread:
            self.thread = Thread(target=self.execution, args=(mem_queue_from_handler,))
        self.thread.start()

    def join(self):
        try:
            self.thread.join()
        except AssertionError:
            logger.error('The thread of this igniter is not in a running state.')

    @staticmethod
    def sleep_for(sleep_time):
        time.sleep(sleep_time)

    def start_workflow(self, mem_queue):
        try:  # this isn't necessary since it's checking with mem_queue.empty()
            workflow = mem_queue.get(block=False)
            response = cromwell_tools.release_workflow(
                cromwell_url=self.cromwell_url,
                workflow_id=workflow.id,
                cromwell_user=self.settings.get('cromwell_user'),
                cromwell_password=self.settings.get('cromwell_password'),
                caas_key=self.settings.get('caas_key')
            )
            if response.status_code != 200:
                logger.warning('Igniter | Failed to start a workflow {0} | {1}'.format(workflow, datetime.now()))
                logger.info('Igniter | {0} | {1}'.format(response.text, datetime.now()))

                if response.status_code == 403:
                    logger.warning('Igniter | Skip sleeping to avoid idle time | {0}'.format(workflow, datetime.now()))
                else:
                    self.sleep_for(self.workflow_start_interval)
            else:
                logger.info('Igniter | Ignited a workflow {0} | {1}'.format(workflow, datetime.now()))
                self.sleep_for(self.workflow_start_interval)

        except queue.Empty:
            logger.info(
                'Igniter | The in-memory queue is empty, wait for the handler to retrieve workflow before next check-in. | {}'.format(
                    datetime.now()))
            self.sleep_for(self.workflow_start_interval)

    def execution(self, mem_queue_from_handler):
        logger.info('Igniter | Initialing an igniter with thread => {0} | {1}'.format(get_ident(), datetime.now()))
        while True:
            self.start_workflow(mem_queue_from_handler)
