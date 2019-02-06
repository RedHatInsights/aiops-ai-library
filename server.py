import logging
import os

from flask import Flask, jsonify, request
from flask.logging import default_handler
from werkzeug.exceptions import BadRequest




def create_application():
    """Create Flask application instance with AWS client enabled."""
    app = Flask(__name__)
    app.config['NEXT_MICROSERVICE_HOST'] = \
        os.environ.get('NEXT_MICROSERVICE_HOST')

    return app


APP = create_application()
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(APP.logger.level)
ROOT_LOGGER.addHandler(default_handler)


@APP.route("/", methods=['POST', 'PUT'])
def index():
    """Pass data to next endpoint."""
    next_service = APP.config['NEXT_MICROSERVICE_HOST']
    try:
        input_data = request.get_json(force=True, cache=False)
    except BadRequest:
        return jsonify(
            status='ERROR',
            message="Unable to parse input data JSON."
        ), 400

    b64_identity = request.headers.get('x-rh-identity')

    # Call the AI Worker here as shown below -
    # AI_Task_worker(input_data, next_service, b64_identity)


    APP.logger.info('Job started')

    return jsonify(
        status='OK',
        message='Volume Type Validation initiated.'
    )


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8004))
    APP.run(port=PORT)
