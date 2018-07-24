import logging
import multiprocessing
import os
import queue
import time
from datetime import datetime
from multiprocessing import Process

import cromwell_tools
from utils import get_settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


class Igniter(object):
    def __init__(self, config_path):
        """Concrete Igniter class for igniting/starting workflows in Cromwell.

        Args:
            config_path (str): Path to the config.json file.
        """
        self.process = None
        self.settings = get_settings(config_path)
        self.cromwell_url = self.settings.get('cromwell_url')

    def spawn_and_start(self, mem_queue_from_handler):
        if not isinstance(mem_queue_from_handler, multiprocessing.queues.Queue):
            raise TypeError('Igniter has to get a shared Queue object from Queue_Handler to start!')

        if not self.process:
            self.process = Process(target=self.execution, args=(mem_queue_from_handler,))
        self.process.start()

    def join(self):
        try:
            self.process.join()
        except AssertionError:
            logger.error('The process of this igniter is not in a running state.')

    @staticmethod
    def sleep_for(sleep_time):
        time.sleep(sleep_time)

    def start_workflow(self, mem_queue):
        if mem_queue.empty():
            logger.info(
                'Igniter | The in-memory queue is empty, wait for the handler to retrieve workflow before next check-in. | {}'.format(
                    datetime.now()))
        else:
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
                else:
                    logger.info('Igniter | Ignited a workflow {0} | {1}'.format(workflow, datetime.now()))

            except queue.Empty:
                logger.info(
                    'Igniter | The in-memory queue is empty, wait for the handler to retrieve workflow before next check-in. | {}'.format(
                        datetime.now()))

    def execution(self, mem_queue_from_handler):
        logger.info('Igniter | Initialing an igniter with process => {0} | {1}'.format(os.getpid(), datetime.now()))
        while True:
            self.start_workflow(mem_queue_from_handler)
            self.sleep_for(self.settings.get('workflow_start_interval'))
