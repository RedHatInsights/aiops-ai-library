import json
import logging
import os
import uuid
from threading import Thread
from urllib3.exceptions import NewConnectionError

import requests
from rad import rad

from prometheus_metrics import METRICS


REQUEST_TIME = METRICS['request_time']
PROCESSING_TIME = METRICS['processing_time']

LOGGER = logging.getLogger('worker')
MAX_RETRIES = 3
FEATURE_LIST = json.loads(os.environ.get('FEATURE_LIST', "[]"))


def _retryable(method: str, *args, **kwargs) -> requests.Response:
    """Retryable HTTP request.

    Invoke a "method" on "requests.session" with retry logic.
    :param method: "get", "post" etc.
    :param *args: Args for requests (first should be an URL, etc.)
    :param **kwargs: Kwargs for requests
    :return: Response object
    :raises: HTTPError when all requests fail
    """
    with requests.Session() as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = getattr(session, method)(*args, **kwargs)

                resp.raise_for_status()
            except (requests.HTTPError, requests.ConnectionError,
                    NewConnectionError) as error:
                LOGGER.warning(
                    'Request failed (attempt #%d), retrying: %s',
                    attempt, str(error)
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


@PROCESSING_TIME.time()
def scikitlearn(data_frame, num_of_tress, sample_size):
    """Use scikitlearn.

    :data_frame: data to be processed
    :num_of_tress: num_of_tress
    :sample_size: sample_size
    :return: IsolationForest and results
    """
    isolation_forest = rad.RADIsolationForest(
        n_estimators=num_of_tress,
        max_samples=sample_size,
        contamination=0.1,
        behaviour="new"
    )
    results = isolation_forest.fit_predict_contrast(
        data_frame,
        training_frame=data_frame
    )
    return isolation_forest, results


def ai_service_worker(
        job: dict,
        next_service: str,
        env: dict,
        b64_identity: str = None,
        ) -> Thread:
    """Outlier detection."""
    @REQUEST_TIME.time()
    def worker() -> None:
        LOGGER.debug('Worker started')

        try:
            account_id, batch_data = job['account'], job['data']
            rows = batch_data['total']
            METRICS['data_size'].observe(rows)
            if rows == 0:
                LOGGER.info(
                    'Job account ID %s: no system in data. Aborting...',
                    account_id
                )
                return
        except KeyError:
            LOGGER.error('Invalid Job data, terminated.')
            return

        batch_id = str(uuid.uuid1())
        LOGGER.info(
            'Job account ID %s (batch ID: %s): Started...',
            account_id, batch_id
        )

        num_trees, sample_size = isolation_forest_params(
            env['num_trees_factor'],
            env['sample_size_factor'],
            rows,
        )

        with METRICS['preparation_time'].time():
            data_frame = rad.inventory_data_to_pandas(
                batch_data, *FEATURE_LIST)
            data_frame, _mapping = rad.preprocess(data_frame)

        isolation_forest, results = scikitlearn(
            data_frame,
            num_trees,
            sample_size,
        )

        with METRICS['report_time'].time():
            reports = isolation_forest.to_report()
        LOGGER.info('Analysis have %s rows in scores', len(results))

        # Build response JSON
        output = {
            'id': batch_id,
            'ai_service': env['ai_service'],
            'data': {
                'account_number': account_id,
                'results': results,
                'feature_list': FEATURE_LIST,
                'common_data': {
                    'charts': reports,
                }
            }
        }

        LOGGER.info(
            'Job ID %s: detection done, publishing to %s ...',
            batch_id, next_service
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
                'Failed to pass data for "%s": %s', batch_id,
                exception
            )
        METRICS['jobs_published'].inc()
        LOGGER.debug('Done, exiting')

    thread = Thread(target=worker)
    thread.start()

    return thread
