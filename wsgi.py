import logging
from server import APP

# Sync logging between Flask and Gunicorn
gunicorn_logger = logging.getLogger('gunicorn.error')
APP.logger.handlers = gunicorn_logger.handlers
APP.logger.setLevel(gunicorn_logger.level)

# Export "application" variable to gunicorn
# pylama:ignore=C0103
application = APP
