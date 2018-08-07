from falcon.queue_handler import QueueHandler
from falcon.igniter import Igniter
import os


if __name__ == '__main__':
    config_path = os.environ.get('CONFIG_PATH')
    handler = QueueHandler(config_path)  # instantiate a concrete `QueueHandler`
    igniter = Igniter(config_path)  # instantiate a concrete `Igniter`

    handler.spawn_and_start()  # start the thread within the handler
    igniter.spawn_and_start(handler)  # start the thread within the igniter by passing the handler into it

    # without monitoring the health of the 2 processes, these joins are trivial
    handler.join()
    igniter.join()
