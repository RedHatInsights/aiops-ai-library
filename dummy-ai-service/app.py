import logging

from flask import Flask, jsonify, request
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
APP = Flask(__name__)

@APP.route("/", methods=['POST', 'PUT'])
def index():
    """Dummy AI service just passing data to next endpoint."""
    data = request.get_json(force=True)

    logger.info('Received data. Black magic routine initiated...')

    data['status'] = 'Processed by AI'
    requests.post(os.environ.get('NEXT_MICROSERVICE_HOST'), json=data)

    logger.info('Done: %s', str(data))

    return jsonify(status="OK")


if __name__ == "__main__":
    APP.run(port=8080, debug=True)
