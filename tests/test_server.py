import json
import requests
from server import APP, VERSION

# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.

# pylint: disable=R0201


def test_route_with_publisher_service_present(mocker):
    """Test index route when publisher service is present."""
    client = APP.test_client(mocker)

    url = '/'

    publisher_response = {'status_code': 200}
    mocker.patch('server._retryable', side_effect=publisher_response)

    response = client.get(url)

    output = {
        "message": "Up and Running",
        "status": "OK",
        "version": VERSION
    }
    assert json.loads(response.get_data()) == output
    assert response.status_code == 200


def test_route_with_publisher_service_error(mocker):
    """Test index route when upload service has an error."""
    client = APP.test_client(mocker)

    url = '/'

    publisher_response = requests.HTTPError(
        mocker.Mock(status=404),
        'not found'
    )
    mocker.patch('server._retryable', side_effect=publisher_response)

    response = client.get(url)

    output = {
        "message": "aiops-publisher not operational",
        "status": "Error",
        "version": VERSION
    }
    assert json.loads(response.get_data()) == output
    assert response.status_code == 500


def test_route_with_publisher_service_absent(mocker):
    """Test index route when upload service has an error."""
    client = APP.test_client(mocker)

    url = '/'

    response = client.get(url)

    output = {
        "message": "aiops-publisher not operational",
        "status": "Error",
        "version": VERSION
    }
    assert json.loads(response.get_data()) == output
    assert response.status_code == 500
