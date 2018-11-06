import logging
import os

from flask import Flask, jsonify, request
import requests

application = Flask(__name__)
gunicorn_logger = logging.getLogger('gunicorn.error')
application.logger.handlers = gunicorn_logger.handlers
application.logger.setLevel(gunicorn_logger.level)


@application.route("/", methods=['POST', 'PUT'])
def index():
    """Dummy AI service just passing data to next endpoint."""
    data = request.get_json(force=True)

    application.logger.info('Received data. Black magic routine initiated...')

    # Identify itself
    data['ai_service'] = 'ai_dummy'

    # Set data to empty list for easier upload
    data['data'] = []

    host = os.environ.get('NEXT_MICROSERVICE_HOST')
    requests.post(f'http://{host}', json=data)

    application.logger.info('Done: %s', str(data))

    return jsonify(
        status='OK',
        message='Data processed by awesome, yet totally useless AI service'
    )


if __name__ == "__main__":
    application.run(port=8080, debug=True)
