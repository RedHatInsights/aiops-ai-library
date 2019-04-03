import logging
from threading import Thread, current_thread

import requests

LOGGER = logging.getLogger()
MAX_RETRIES = 3


def _retryable(method: str, *args, **kwargs) -> requests.Response:
    """Retryable HTTP request.

    Invoke a "method" on "requests.session" with retry logic.
    :param method: "get", "post" etc.
    :param *args: Args for requests (first should be an URL, etc.)
    :param **kwargs: Kwargs for requests
    :return: Response object
    :raises: HTTPError when all requests fail
    """
    thread = current_thread()

    with requests.Session() as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = getattr(session, method)(*args, **kwargs)

                resp.raise_for_status()
            except (requests.HTTPError, requests.ConnectionError) as e:
                LOGGER.warning(
                    '%s: Request failed (attempt #%d), retrying: %s',
                    thread.name, attempt, str(e)
                )
                continue
            else:
                return resp

    raise requests.HTTPError('All attempts failed')


# define the AI_Task_worker here as shown below -
# def volume_type_validation_worker(
#         job: dict,
#         next_service: str,
#         b64_identity: str = None
# )

def ai_worker(
        job: dict,
        next_service: str,
        ai_service: str,
        b64_identity: str = None
) -> Thread:
    """Validate Volume Types."""
    def worker() -> None:
        thread = current_thread()
        LOGGER.debug('%s: Worker started', thread.name)

        try:
            batch_id, batch_data = job['id'], job['data']  # noqa
        except KeyError:
            LOGGER.error("%s: Invalid Job data, terminated.", thread.name)
            return

        LOGGER.info('%s: Job ID %s: Started...', thread.name, batch_id)

        # AI Processing of input data in `job` goes here
        # Store the AI Results in `output`
        output = {
            'id': batch_id,
            'ai_service': ai_service,
            'data': 'RESULT_DATA'
        }

        # Pass to the next service
        try:
            _retryable(
                'post',
                next_service,
                json=output,
                headers={"x-rh-identity": b64_identity}
            )
        except requests.HTTPError as exception:
            LOGGER.error(
                '%s: Failed to pass data for "%s": %s',
                thread.name, batch_id, exception
            )

        LOGGER.debug('%s: Done, exiting', thread.name)

    thread = Thread(target=worker)
    thread.start()

    return thread
