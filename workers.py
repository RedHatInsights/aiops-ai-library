import logging
import uuid
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


def isolation_forest_params(trees_factor, sample_factor, data_rows):
    """Fine tune parameters for IsolationForest.

    :param num_trees: num_trees factor
    :params sample_size: sample_size factor
    :data_rows: data size to be trained
    :return: the tuned values
    """
    if 0.001 < trees_factor < 1.0:
        num_trees = data_rows * trees_factor
    else:
        num_trees = data_rows * 0.2
    if 0.001 < sample_factor < 1.0:
        sample_size = data_rows * sample_factor
    else:
        sample_size = data_rows * 0.2
    return int(num_trees), int(sample_size)


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
            account_id, batch_data = job['account'], job['data']
            rows = batch_data['total']
            if rows == 0:
                LOGGER.info(
                    '%s: Job account ID %s: no system in data. Aborting...',
                    thread.name, account_id
                )
                return
        except KeyError:
            LOGGER.error('%s: Invalid Job data, terminated.', thread.name)
            return

        batch_id = str(uuid.uuid1())
        LOGGER.info(
            '%s: Job account ID %s (batch ID: %s): Started...',
            thread.name, account_id, batch_id
        )

        num_trees, sample_size = isolation_forest_params(
            env['num_trees_factor'],
            env['sample_size_factor'],
            rows,
        )

        data_frame = rad.inventory_data_to_pandas(batch_data)
        data_frame, _mapping = rad.preprocess(data_frame)
        isolation_forest = rad.IsolationForest(
            data_frame,
            num_trees,
            sample_size,
        )
        results = isolation_forest.predict(
            data_frame,
            min_score=env['min_score'],
        )

        LOGGER.info('Analysis have %s rows in scores', len(results))

        # Build response JSON
        output = {
            'id': batch_id,
            'ai_service': env['ai_service'],
            'data': {
                'account_number': account_id,
                'results': results,
                'common_data': {
                    'charts': isolation_forest.to_report(),
                }
            }
        }

        LOGGER.info(
            '%s: Job ID %s: detection done, publishing to %s ...',
            thread.name, batch_id, next_service
        )

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
