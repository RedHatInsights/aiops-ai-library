import logging

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

    data['status'] = 'Processed by AI'
    requests.post(os.environ.get('NEXT_MICROSERVICE_HOST'), json=data)

    application.logger.info('Done: %s', str(data))

    return jsonify(status="OK")


if __name__ == "__main__":
    application.run(port=8080, debug=True)
