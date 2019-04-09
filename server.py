import logging
import os

from flask import Flask, jsonify, request
from flask.logging import default_handler
from werkzeug.exceptions import BadRequest

from workers import ai_service_worker


def create_application():
    """Create Flask application instance."""
    app = Flask(__name__)
    app.config['AI_SERVICE'] = os.environ.get('AI_SERVICE')
    app.config['NEXT_SERVICE_URL'] = os.environ.get('NEXT_SERVICE_URL')
    app.config['IF_NUM_TREES_FACTOR'] = \
        float(os.environ.get('IF_NUM_TREES_FACTOR') or '0.2')
    app.config['IF_SAMPLE_SIZE_FACTOR'] = \
        float(os.environ.get('IF_SAMPLE_SIZE_FACTOR') or '0.2')
    return app


APP = create_application()
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(APP.logger.level)
ROOT_LOGGER.addHandler(default_handler)


@APP.route("/", methods=['POST', 'PUT'])
def index():
    """Pass data to next endpoint."""
    next_service = APP.config['NEXT_SERVICE_URL']
    env = {
        'ai_service': APP.config['AI_SERVICE'],
        'num_trees_factor': APP.config['IF_NUM_TREES_FACTOR'],
        'sample_size_factor': APP.config['IF_SAMPLE_SIZE_FACTOR'],
    }

    try:
        input_data = request.get_json(force=True, cache=False)
    except BadRequest:
        return jsonify(
            status='ERROR',
            message="Unable to parse input data JSON.",
        ), 400

    b64_identity = request.headers.get('x-rh-identity')

    ai_service_worker(
        input_data,
        next_service,
        env,
        b64_identity,
    )

    APP.logger.info('Job started')

    return jsonify(
        status='OK',
        message='Outlier detection initiated.',
    )


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8005))
    APP.run(port=PORT)
