import os
import threading
from flask import Flask
from igniter import Igniter
from queue_handler import QueueHandler


FALCON_THREAD_NAMES = ['queueHandler', 'igniter']

app = Flask(__name__)


@app.route("/health")
def status():
    active_threads = threading.enumerate()
    active_falcon_threads = [thread for thread in active_threads if thread.name in FALCON_THREAD_NAMES]
    for thread in active_falcon_threads:
        if not thread.is_alive():
            return "Error in {} thread {}".format(thread.name, thread.ident)
    healthy_threads = ['{}-{}'.format(t.name, t.ident) for t in active_falcon_threads]
    return 'Falcon threads: ' + ', '.join(healthy_threads)


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
