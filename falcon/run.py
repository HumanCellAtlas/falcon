import os
from flask import Flask
from falcon.igniter import Igniter
from falcon.queue_handler import QueueHandler
from falcon.routes import status

app = Flask(__name__, static_url_path='')

# prevent cached responses
@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    removing caching
    """
    r.headers["Cache-Control"] = "no-store, max-age=0"
    r.headers["Pragma"] = "no-cache"
    return r

# Define endpoint and assign to status function
app.add_url_rule("/health", "health", status)

config_path = os.environ.get('CONFIG_PATH')
handler = QueueHandler(config_path)  # instantiate a concrete `QueueHandler`
igniter = Igniter(config_path)  # instantiate a concrete `Igniter`


handler.spawn_and_start()  # start the thread within the handler
igniter.spawn_and_start(
    handler
)  # start the thread within the igniter by passing the handler into it
