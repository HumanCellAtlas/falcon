import threading
from falcon import app

FALCON_THREAD_NAMES = ['queueHandler', 'igniter']


@app.route("/health")
def status():
    active_threads = threading.enumerate()
    active_falcon_threads = [thread for thread in active_threads if thread.name in FALCON_THREAD_NAMES]
    for thread in active_falcon_threads:
        if not thread.is_alive():
            return "Error in {} thread {}".format(thread.name, thread.ident)
    healthy_threads = ['{}-{}'.format(t.name, t.ident) for t in active_falcon_threads]
    return 'Falcon threads: ' + ', '.join(healthy_threads)