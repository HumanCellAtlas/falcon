from queue_handler import Queue_Handler
from igniter import Igniter
import os


if __name__ == '__main__':
    config_path = os.environ.get('CONFIG_PATH')
    handler = Queue_Handler(config_path)
    igniter = Igniter(config_path)

    handler.spawn_and_run()
    igniter.spawn_and_run(handler.workflow_queue)

    # without monitoring the health of the 2 processes, these joins are trivial
    handler.join()
    igniter.join()
