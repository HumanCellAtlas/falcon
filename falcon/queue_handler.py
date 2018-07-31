import logging
import time
from datetime import datetime
from threading import Thread, get_ident
from queue import Queue

from cromwell_tools import cromwell_tools
from falcon import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('falcon.{module_path}'.format(module_path=__name__))


class Workflow(object):
    """A simple data structure for hosting workflow data.

    Besides the features for de-duplication, this class also utilizes a smaller size of chunk in memory.
    """

    def __init__(self, workflow_id, bundle_uuid=None, bundle_version=None):
        self.id = workflow_id
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return str(self.id)

    def __eq__(self, other):
        """
        Note: In the future, if we want to add advanced de-duplication feature to the service, besides asserting
        workflow id between 2 `Workflow` objects, we might also want to check if they have the same `bundle_uuid`
        and `bundle_version`.
        """
        if isinstance(other, Workflow):
            return self.id == other.id
        return False


class QueueHandler(object):
    """
    A concrete queue handler class.
    """

    def __init__(self, config_object):
        self.workflow_queue = self.create_empty_queue(-1)  # use infinite for the size of the queue for now
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
        """
        Starts the thread, which is an instance variable. If thread has not been created, spawns it and then starts it.
        """
        if not self.thread:
            self.thread = Thread(target=self.execution)
        self.thread.start()

    def join(self):
        """
        A wrapper function around `threading.Thread.join()`.
        """
        try:
            self.thread.join()
        except (AttributeError, AssertionError):
            logger.error('The thread of this queue handler is not in a running state.')

    def retrieve_workflows(self, query_dict):
        """
        Retrieve the latest list of metadata of all "On Hold" workflows from Cromwell.

        Args:
            query_dict (dict): A dictionary that contains valid query parameters which can be accepted by the Cromwell
            /query  endpoint.
        Returns:
            workflow_metas (None or list): Will be None if it gets a non 200 code from Cromwell, otherwise will be a
            list of workflow metadata dict blocks. e.g.
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
        response = cromwell_tools.query_workflows(
            cromwell_url=self.cromwell_url,
            query_dict=query_dict,
            cromwell_user=self.settings.get('cromwell_user'),
            cromwell_password=self.settings.get('cromwell_password'),
            caas_key=self.settings.get('caas_key')
        )
        if response.status_code != 200:
            logger.warning('Queue | Failed to retrieve workflows from Cromwell | {0} | {1}'.format(response.text, datetime.now()))
            workflow_metas = None
        else:
            workflow_metas = response.json()['results']
            num_workflows = len(workflow_metas)
            logger.info('Queue | Retrieved {0} workflows from Cromwell. | {1}'.format(num_workflows, datetime.now()))
            logger.debug('Queue | {0} | {1}'.format(workflow_metas, datetime.now()))  # TODO: remove this or not?
        return workflow_metas

    def prepare_workflows(self, workflow_metas):
        """
        This function will figure out the correct order of the workflow metadata object, parse them and convert to a
        iterator object that contains assembled `Workflow` objects.

        Args:
            workflow_metas (list): A list of workflow metadata dict blocks. e.g.
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
            workflows_iterator (map iterator): An iterator that applies `_assemble_workflow()` to every item of
            the workflow_metas, yielding the result `Workflow` instance.
        """
        if not self.is_workflow_list_in_oldest_first_order(workflow_metas):
            workflow_metas = workflow_metas[::-1]
        workflows_iterator = map(self._assemble_workflow, workflow_metas)
        return workflows_iterator

    def enqueue(self, workflows):
        """
        Put workflows into the in-memory queue object, which is an instance variable.

        Args:
            workflows (iterable): An iterable(list or iterator) object that contains all `Workflow` instances that need
            to be put in the in-memory queue.
        """
        for workflow in workflows:
            logger.debug('Queue | Enqueuing workflow {0} | {1}'.format(workflow, datetime.now()))
            self.workflow_queue.put(workflow)  # TODO: Implement and add de-duplication logic here

    def rebuild_queue(self, queue):
        """
        Move the reference from the old queue to the new object to maintain the pointer integrity for the instance
        variable `self.workflow_queue`. Make this a separate function so it's easier to test.

        Args:
            queue: A concrete queue object which will replace queue that is currently referred by the handler.
        """
        self.workflow_queue = queue

    def execution(self):
        logger.info(
            'Queue | Initializing the queue handler with thread => {0} | {1}'.format(get_ident(), datetime.now()))

        while True:
            workflow_metas = self.retrieve_workflows(self.cromwell_query_dict)
            if workflow_metas:  # This could happen when getting either non-200 codes or 0 workflow from Cromwell
                workflows = self.prepare_workflows(workflow_metas)

                self.rebuild_queue(self.create_empty_queue(-1))  # This must happen before `enqueue()` is called!
                self.enqueue(workflows)
            else:
                logger.info(
                    'Queue | Cannot fetch any workflow from Cromwell, go back to sleep and wait for next attempt. | {0}'
                        .format(datetime.now())
                )
            self.sleep_for(self.queue_update_interval)

    @staticmethod
    def create_empty_queue(max_queue_size=-1):
        """
        This function works as a factory which returns a concrete Queue object. Modifying this function gives you
        the ability to plug in different implementations of Queue object for the `QueueHandler` instances.

        Args:
            max_queue_size (int): For the current `queue.Queue()` implementation, this field is an integer that sets
            the upperbound limit on the number of items that can be placed in the queue. Insertion will block once
            this size has been reached, until queue items are consumed. If maxsize is less than or equal to zero,
            the queue size is infinite.

        Returns:
            queue.Queue: A concrete `Queue` instance.
        """
        return Queue(maxsize=max_queue_size)

    @staticmethod
    def _assemble_workflow(workflow_meta):
        """
        This is a helper function that parses a block of workflow metadata object and assembles it to a `Workflow`
        instance.

        Args:
            workflow_meta (dict): A dictionary that contains the metadata of a workflow, usually this is returned from
            Cromwell and parsed by JSON utils. An example block would look like:
            ```
            {
                "name": "WorkflowName1",
                "id": "xxx1",
                "submission": "2018-01-01T23:49:40.620Z",
                "status": "Succeeded",
                "end": "2018-07-12T00:37:12.282Z",
                "start": "2018-07-11T23:49:48.384Z"
            }
            ```

        Returns:
            Workflow: A concrete `Workflow` instance that has necessary properties.
        """
        workflow_id = workflow_meta.get('id')
        workflow_labels = workflow_meta.get('labels')  # TODO: Integrate this field into Workflow class
        workflow_bundle_uuid = workflow_labels.get('bundle-uuid') if isinstance(workflow_labels, dict) else None
        workflow_bundle_version = workflow_labels.get('bundle-version') if isinstance(workflow_labels, dict) else None
        workflow = Workflow(workflow_id, workflow_bundle_uuid, workflow_bundle_version)
        return workflow

    @staticmethod
    def is_workflow_list_in_oldest_first_order(workflow_list):
        """
        This function will figure out how is the `workflow_list` is sorted.
        From Cromwell v34 (https://github.com/broadinstitute/cromwell/releases/tag/34), Query results will
        be returned in reverse chronological order, with the most-recently submitted workflows returned first, which
        is a different behavior from the older versions.

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
    def sleep_for(sleep_time):
        time.sleep(sleep_time)

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
