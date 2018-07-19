from queue_handler import Queue_Handler
from igniter import Igniter
import os


if __name__ == '__main__':
    handler = Queue_Handler(os.environ.get('CONFIG_PATH'))
    igniter = Igniter(os.environ.get('CONFIG_PATH'))

    handler.spawn_and_run()
    igniter.spawn_and_run(handler.workflow_queue)

    handler.join()
    igniter.join()
