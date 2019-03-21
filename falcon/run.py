import os
from flask import Flask
from falcon.igniter import Igniter
from falcon.queue_handler import QueueHandler

app = Flask(__name__)

from falcon import routes  # This must come after the app instantiation to avoid circular imports


config_path = os.environ.get('CONFIG_PATH')
handler = QueueHandler(config_path)  # instantiate a concrete `QueueHandler`
igniter = Igniter(config_path)  # instantiate a concrete `Igniter`

handler.spawn_and_start()  # start the thread within the handler
igniter.spawn_and_start(handler)  # start the thread within the igniter by passing the handler into it
