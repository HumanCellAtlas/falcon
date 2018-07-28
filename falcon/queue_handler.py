import logging
import time
from datetime import datetime
from threading import Thread, Lock, get_ident
from queue import Queue

from cromwell_tools import cromwell_tools
from falcon import settings


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


class Workflow(object):
    """A simple data structure for hosting workflow data.

    Besides the features for de-duplication, this class also utilizes a smaller size of chunk in memory.
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
            return self.id == other.id
        return False


class Queue_Handler(object):
    """A concrete queue handler.
    """

    def __init__(self, config_object):
        self.workflow_queue = self.obtainQueue(-1)  # use infinite for the size of the queue for now
        self.thread = None
        self.settings = settings.get_settings(config_object)
        self.queue_update_interval = self.settings.get('queue_update_interval')
        self.cromwell_url = self.settings.get('cromwell_url')
        self.cromwell_query_dict = {
            'status': 'On Hold',
            'additionalQueryResultFields': 'labels',
            'includeSubworkflows': 'false'  # TODO: should accept False, this is a bug in cromwell_tools, need fix there
        }

    def spawn_and_start(self):
        if not self.thread:
            self.thread = Thread(target=self.execution)
        self.thread.start()

    def join(self):
        try:
            self.thread.join()
        except (AttributeError, AssertionError):
            logger.error('The thread of this queue handler is not in a running state.')

    @staticmethod
    def obtainQueue(max_queue_size=-1):
        return Queue(maxsize=max_queue_size)

    def sleep_for(self, sleep_time):
        time.sleep(sleep_time)

    def retrieve_workflows(self, query_dict):
        """Retrieve the latest set of "On Hold" workflows from Cromwell and put them in the in-memory queue.

        Args:
            query_dict (dict): A dictionary that contains valid query parameters which can be accepted by the Cromwell
            /query  endpoint.
        """
        # workflows should be a list that ordered by submission time, oldest first, not true anymore after Cromwell v34
        response = cromwell_tools.query_workflows(
            cromwell_url=self.cromwell_url,
            query_dict=query_dict,
            cromwell_user=self.settings.get('cromwell_user'),
            cromwell_password=self.settings.get('cromwell_password'),
            caas_key=self.settings.get('caas_key')
        )

        if response.status_code != 200:
            logger.warning('Queue | Failed to retrieve workflows from Cromwell | {0} | {1}'.format(response.text, datetime.now()))
            return []
        else:
            return response.json()['results']

    def enqueue(self, workflow_metas):
        """Put workflows into the in-memory queue.

        Args:
            workflow_metas (list): A list of workflow metadata objects, e.g.
            ```
                [
                    {
                        "name": "WorkflowName1",
                        "id": "xxx1",
                        "submission": "2018-01-01T23:49:40.620Z",
                        "status": "Succeeded",
                        "end": "2018-07-12T00:37:12.282Z",
                        "start": "2018-07-11T23:49:48.384Z"
                    },
                    {
                        "name": "WorkflowName2",
                        "id": "xxx2",
                        "submission": "2018-01-01T23:49:42.171Z",
                        "status": "Succeeded",
                        "end": "2018-07-12T00:31:27.273Z",
                        "start": "2018-07-11T23:49:48.385Z"
                    }
                ]
            ```
        """
        workflow_num = len(workflow_metas)

        if workflow_num:
            logger.debug('Queue | {0} | {1}'.format(workflow_metas, datetime.now()))  #TODO: remove this or not?

            logger.info('Queue | Retrieved {0} workflows from Cromwell. | {1}'.format(workflow_num, datetime.now()))

            if not self.is_workflow_list_in_oldest_first_order(workflow_metas):
                workflow_metas = workflow_metas[::-1]

            # Move the reference to the old queue to the new object to maintain the pointer integrity for the igniter
            # This can be moved out from the current block, so the handler will have 2 Queue objects before it
            # moves the reference/pointer. That way the igniter will either  start more stale worklfows potentially or
            # be idle during handler is enqueuing
            self.workflow_queue = self.obtainQueue(-1)

            for workflow_meta in workflow_metas:
                workflow_id = workflow_meta.get('id')
                workflow_labels = workflow_meta.get('labels')  # TODO: Integrate this field into Workflow class
                workflow_bundle_uuid = workflow_labels.get('bundle-uuid') if isinstance(workflow_labels, dict) else None

                workflow = Workflow(workflow_id, workflow_bundle_uuid)

                logger.debug(
                    'Queue | Enqueuing workflow {0} | {1}'.format(workflow, datetime.now()))

                self.workflow_queue.put(workflow)  # TODO: Implement and add de-duplication logic here
        else:
            logger.info(
                'Queue | Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt. | {0}'.format(
                    datetime.now()))

    def execution(self):
        logger.info(
            'Queue | Initialing the queue handler with thread => {0} | {1}'.format(get_ident(), datetime.now()))

        while True:
            self.enqueue(
                self.retrieve_workflows(self.cromwell_query_dict)
            )
            self.sleep_for(self.queue_update_interval)

    @staticmethod
    def is_workflow_list_in_oldest_first_order(workflow_list):
        """Placeholder.

        From Cromwell v34 (https://github.com/broadinstitute/cromwell/releases/tag/34), Query results will
        be returned in reverse chronological order, with the most-recently submitted workflows returned first,
        the logic here need to be updated.

        Args:
            workflow_list (list): A list of workflow metadata objects, e.g.
            ```
                [
                    {
                        "name": "WorkflowName1",
                        "id": "xxx1",
                        "submission": "2018-01-01T23:49:40.620Z",
                        "status": "Succeeded",
                        "end": "2018-07-12T00:37:12.282Z",
                        "start": "2018-07-11T23:49:48.384Z"
                    },
                    {
                        "name": "WorkflowName2",
                        "id": "xxx2",
                        "submission": "2018-01-01T23:49:42.171Z",
                        "status": "Succeeded",
                        "end": "2018-07-12T00:31:27.273Z",
                        "start": "2018-07-11T23:49:48.385Z"
                    }
                ]
            ```

        Returns:
            bool: The return value. True if the workflow_list is sorted oldest first, False otherwise.
        """
        CROMWELL_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        try:
            head = datetime.strptime(str(workflow_list[0].get('submission')), CROMWELL_DATETIME_FORMAT)
            tail = datetime.strptime(str(workflow_list[-1].get('submission')), CROMWELL_DATETIME_FORMAT)
            if head <= tail:
                return True
        except ValueError:
            logger.error(
                'Queue | An error happened when try to parse the submission timestamps, will assume oldest first for'
                ' the workflows returned from Cromwell | {0}'.format(datetime.now()))
            return True
        return False

    @staticmethod
    def shallow_deduplicate():
        """A placeholder function for de-duplication logic, not implemented yet.

        This shallow de-duplication should only search given bundle-uuid and bundle-version in the current domain,
        e.g. notifications in the queue.
        """
        return NotImplemented

    @staticmethod
    def deep_deduplicate():
        """A placeholder function for de-duplication logic, not implemented yet.

        This deep de-duplication should search the given bundle-uuid and bundle-version in the whole history.
        """
        return NotImplemented
