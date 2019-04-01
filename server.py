import logging
import os

from flask import Flask, jsonify, request
from flask.logging import default_handler
from werkzeug.exceptions import BadRequest


from workers import idle_cost_savings_worker


def create_application():
    """Create Flask application instance with AWS client enabled."""
    app = Flask(__name__)
    app.config['NEXT_MICROSERVICE_HOST'] = \
        os.environ.get('NEXT_MICROSERVICE_HOST')
    app.config['AI_SERVICE'] = \
        os.environ.get('AI_SERVICE')

    return app


APP = create_application()
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(APP.logger.level)
ROOT_LOGGER.addHandler(default_handler)


@APP.route("/", methods=['POST', 'PUT'])
def index():
    """Pass data to next endpoint."""
    next_service = APP.config['NEXT_MICROSERVICE_HOST']
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
