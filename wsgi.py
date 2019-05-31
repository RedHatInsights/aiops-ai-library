import logging
import os
import sys

from gunicorn.arbiter import Arbiter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | p-%(process)d | t-%(thread)d | %(name)s |'
           ' %(levelname)-5s | %(message)s'
)
gunicorn_logger = logging.getLogger('gunicorn.error')

# all gunicorn processes in a given instance need to access a common
# folder in /tmp where the metrics can be recorded
PROMETHEUS_MULTIPROC_DIR = '/tmp/aiops_outlier_detection'

try:
    os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)
    os.environ['prometheus_multiproc_dir'] = PROMETHEUS_MULTIPROC_DIR
    from server import APP
except IOError as e:
    # this is a non-starter for scraping metrics in the
    # Multiprocess Mode (Gunicorn)
    # terminate if there is an exception here
    gunicorn_logger.error(
        "Error while creating prometheus_multiproc_dir: %s", e
    )
    sys.exit(Arbiter.APP_LOAD_ERROR)

# Export "application" variable to gunicorn
# pylama:ignore=C0103
application = APP
