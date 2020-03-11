from datetime import datetime, timedelta
import os
from flask import abort
from flask import render_template
from falcon.settings import docRootPath, docRootFile, MAX_DELAY


def status():
    """
    This function reads a status report file creation date and
    Compares it to the current time
    Returns: render html file or abort (HTTP code 500) if time
             difference is greater than max delay
    """

    try:
        # Get TimeStamp
        now = datetime.today()

        # read status report.html modified datetime
        file_mod_time = datetime.fromtimestamp(
            os.stat(docRootPath + docRootFile).st_mtime
        )  # This is a datetime.datetime object!

        # Define max delay to 5 mins
        max_delay = timedelta(minutes=MAX_DELAY)

        # if reached max delay abort else render status report file
        if now - file_mod_time > max_delay:
            abort(500, "reached max delay")
        else:
            return render_template(docRootFile)

    except Exception as exc:
        abort(500, exc)
