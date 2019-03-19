import os
import threading
from falcon import app
from falcon.igniter import Igniter
from falcon.queue_handler import QueueHandler


class WebThread(threading.Thread):
    def start(self):
        app.run(port='8080', host='0.0.0.0', debug=True)


if __name__ == '__main__':
    config_path = os.environ.get('CONFIG_PATH')
    handler = QueueHandler(config_path)  # instantiate a concrete `QueueHandler`
    igniter = Igniter(config_path)  # instantiate a concrete `Igniter`
    w = WebThread()

    handler.spawn_and_start()  # start the thread within the handler
    igniter.spawn_and_start(handler)  # start the thread within the igniter by passing the handler into it
    w.start()

    # without monitoring the health of the 2 processes, these joins are trivial
    handler.join()
    igniter.join()
