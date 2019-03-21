from flask import Flask

falcon_app = Flask(__name__)

from . import routes  # Keep at bottom of file to avoid circular imports
