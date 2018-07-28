from falcon.queue_handler import Queue_Handler
from falcon.igniter import Igniter
import os


if __name__ == '__main__':
    config_path = os.environ.get('CONFIG_PATH')
    handler = Queue_Handler(config_path)  # instantiate a concrete queue_handler
    igniter = Igniter(config_path)  # instantiate a concrete igniter

    handler.spawn_and_start()  # start the thread of the queue_handler
    igniter.spawn_and_start(handler)  # start the thread of the igniter by passing the queue_handler into it

    # without monitoring the health of the 2 processes, these joins are trivial
    handler.join()
    igniter.join()
