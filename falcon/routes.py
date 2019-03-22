import json
import threading
from falcon.run import app

FALCON_THREAD_NAMES = ('queueHandler', 'igniter')


@app.route("/health")
def status():
    active_threads = threading.enumerate()
    active_falcon_threads = [
        thread for thread in active_threads if thread.name in FALCON_THREAD_NAMES
    ]
    for thread in active_falcon_threads:
        if not thread.is_alive():
            return "Error in {} thread {}".format(thread.name, thread.ident)
    falcon_thread_status = {t.name: t.ident for t in active_falcon_threads}
    return json.dumps(falcon_thread_status)
