import json
import threading
from flask import abort

FALCON_THREAD_NAMES = ('queueHandler', 'igniter')


def status():
    active_threads = {thread.name: thread for thread in threading.enumerate()}
    active_falcon_threads = {}
    for falcon_thread_name in FALCON_THREAD_NAMES:
        thread = active_threads.get(falcon_thread_name)
        if falcon_thread_name not in active_threads.keys():
            abort(500)
        elif not thread.is_alive():
            abort(500)
        else:
            display_name = '{}-thread'.format(falcon_thread_name)
            active_falcon_threads[display_name] = str(thread.ident)
    return json.dumps(active_falcon_threads)
