import logging
import os
import requests

from flask import Flask, jsonify, request
from flask.logging import default_handler
from werkzeug.exceptions import BadRequest
from urllib3.exceptions import NewConnectionError

from workers import _retryable


from workers import idle_cost_savings_worker


def create_application():
    """Create Flask application instance with AWS client enabled."""
    app = Flask(__name__)
    app.config['NEXT_SERVICE_URL'] = os.environ.get('NEXT_SERVICE_URL')
    app.config['AI_SERVICE'] = os.environ.get('AI_SERVICE')

    return app


APP = create_application()
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(APP.logger.level)
ROOT_LOGGER.addHandler(default_handler)

VERSION = "0.0.1"


@APP.route("/", methods=['GET'])
def get_root():
    """Root Endpoint for Liveness/Readiness check."""
    next_service = APP.config['NEXT_SERVICE_URL']
    try:
        _retryable('get', f'{next_service}')
        status = 'OK'
        message = 'Up and Running'
        status_code = 200
    except (requests.HTTPError, NewConnectionError):
        status = 'Error'
        message = 'aiops-publisher not operational'
        status_code = 500

    return jsonify(
        status=status,
        version=VERSION,
        message=message
    ), status_code


@APP.route("/", methods=['POST', 'PUT'])
def index():
    """Pass data to next endpoint."""
    next_service = APP.config['NEXT_SERVICE_URL']
    ai_service = APP.config['AI_SERVICE']
    try:
        input_data = request.get_json(force=True, cache=False)

        # Note: For insights data use `request.get_data()` instead
        # raw_data = request.get_data()
        # job_id = request.headers['source_id']
        # except (BadRequest, KeyError):
    except BadRequest:
        return jsonify(
            status='ERROR',
            message="Unable to parse input data JSON."
        ), 400

    b64_identity = request.headers.get('x-rh-identity')

    # Call the AI Worker here as shown below -
    # AI_Task_worker(input_data, next_service, b64_identity)

    # Note: For insights data use `raw_data`
    # AI_Task_worker(job_id, raw_data, next_service, b64_identity)

    idle_cost_savings_worker(
        input_data,
        next_service,
        ai_service,
        b64_identity
    )

    APP.logger.info('Job started')

    return jsonify(
        status='OK',
        message='Idle Cost Savings Analysis initiated.'
    )


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8004))
    APP.run(port=PORT)
