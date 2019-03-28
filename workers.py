import logging
import math
from threading import Thread, current_thread

import requests

from rad import rad


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
            except (requests.HTTPError, requests.ConnectionError) as error:
                LOGGER.warning(
                    '%s: Request failed (attempt #%d), retrying: %s',
                    thread.name, attempt, str(error)
                )
                continue
            else:
                return resp

    raise requests.HTTPError('All attempts failed')

def ai_service_worker(
        job: dict,
        next_service: str,
        env: dict,
        b64_identity: str = None,
    ) -> Thread:
    """Outlier detection."""

    def worker() -> None:
        thread = current_thread()
        LOGGER.debug('%s: Worker started', thread.name)

        try:
            batch_id, batch_data = job['account'], job['data']
            rows = batch_data['total']
        except KeyError:
            LOGGER.error('%s: Invalid Job data, terminated.', thread.name)
            return

        if rows == 0:
            LOGGER.info('%s: Job account ID %s: no system in data. Aborting...', thread.name, batch_id)
            return
        LOGGER.info('%s: Job account ID %s: Started...', thread.name, batch_id)

        # TODO: need to review these defaults
        num_trees = env['num_trees']
        if num_trees > rows/2 or num_trees == 0:
            num_trees = int(math.log(rows))

        sample_size = env['sample_size']
        if sample_size > rows/2 or sample_size == 0:
            sample_size = int(rows/num_trees)
        data_frame = rad.inventory_data_to_pandas(batch_data)
        data_frame, _mapping = rad.preprocess(data_frame)
        isolation_forest = rad.IsolationForest(
            data_frame,
            num_trees,
            sample_size,
        )
        result = isolation_forest.predict(data_frame)

        LOGGER.info(f'Analysis have {len(result)} rows in scores')

        # Build response JSON
        output = {
            'id': batch_id,
            'ai_service': env['ai_service'],
            'data': {
                'scores': result.to_dict(),
            }
        }

        LOGGER.info(
            '%s: Job ID %s: detection done, publishing...',
            thread.name, batch_id
        )

        # Pass to the next service
        try:
            _retryable(
                'post',
                f'http://{next_service}',
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
