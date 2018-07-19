import logging
import os
import time
from datetime import datetime
from multiprocessing import Process, Queue

import cromwell_tools
from utils import get_settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


class Workflow(object):
    """A simple data structure for hosting workflow data.

    Besides the features for de-duplication, this class also utilizes a smaller size of chunk in memory,
    comparing to dict or tuple.
    """

    def __init__(self, workflow_id, bundle_uuid=None):
        self.id = workflow_id
        self.bundle_uuid = bundle_uuid

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return str(self.id)

    def __eq__(self, other):
        if isinstance(other, Workflow):
            return self.id == other.id and self.bundle_uuid == other.bundle_uuid
        return False


class Queue_Handler(object):
    """An abstract queue handler.

    Note: When an object is put on a queue, the object is pickled and a background thread later flushes the pickled
    data to an underlying pipe. This has some consequences which are a little surprising, but should not cause any
    practical difficulties - if they really bother you then you can instead use a queue created with a manager.
        1. After putting an object on an empty queue there may be an infinitesimal delay before the queue’s empty()
        method returns False and get_nowait() can return without raising queue.Empty.
        2. If multiple processes are enqueuing objects, it is possible for the objects to be received at the other
        end out-of-order. However, objects enqueued by the same process will always be in the expected order with
        respect to each other.

    """

    def __init__(self, config_object):
        self.workflow_queue = Queue(maxsize=0)  # use infinite for the size of the queue for now
        self.process = None
        self.last_submission = None
        self.settings = get_settings(config_object)
        self.cromwell_url = self.settings.get('cromwell_url')
        self.cromwell_query_dict = {
            'status': 'On Hold',
            'additionalQueryResultFields': 'labels',
            'includeSubworkflows': False
        }

    def spawn_and_run(self):
        if not self.process:
            self.process = Process(target=self.execution)
        self.process.start()

    def join(self):
        self.process.join()

    def sleep_for(self, sleep_time):
        time.sleep(sleep_time)

    def retrieve_queue(self, query_dict):
        # prepare the query_dict parameter for sending requests to Cromwell
        # workflows should be a list that ordered by submission time, oldest first
        workflows = cromwell_tools.query_workflows(
            cromwell_url=self.cromwell_url,
            query_dict=query_dict,
            cromwell_user=self.settings.get('cromwell_user'),
            cromwell_password=self.settings.get('cromwell_password'),
            caas_key=self.settings.get('caas_key')
        ).json()['results']

        logger.info('Queue | Retrieved {0} workflows from Cromwell. | {1}'.format(len(workflows), datetime.now()))

        # store the latest submission timestamp to save computation time for later updates
        self.latest_on_hold = workflows[-1].get('submission')

        # placeholder for the de-duplication logic
        for workflow in workflows:
            workflow = Workflow(workflow.get('workflow_id'), workflow.get('bundle-uuid'))

            logger.debug(
                'Queue | Enqueuing workflow {0} | {1}'.format(workflow, datetime.now()))

            self.workflow_queue.put(self.deep_deduplicate(
                self.shallow_deduplicate(workflow)
            ))

    def execution(self):
        logger.info(
            'Queue | Initialing the queue handler with process => {0} | {1}'.format(os.getpid(), datetime.now()))

        while True:
            if self.last_submission:
                # make sure it only queries the workflows submitted after the last retrieve, if applicable
                self.cromwell_query_dict['submission'] = self.last_submission  # this is not a atomic manipulation!
            self.retrieve_queue(self.cromwell_query_dict)
            self.sleep_for(self.settings.get('queue_update_interval'))

    @staticmethod
    def shallow_deduplicate(ls):
        """Shallow de-duplication should only search given bundle-uuid and bundle-version in the current domain, e.g. notifications in the queue."""
        return ls

    @staticmethod
    def deep_deduplicate(ls):
        """Deep de-duplication should search the given bundle-uuid and bundle-version in the whole history."""
        return ls
