import logging
import math
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


def compile_scores(scores):
    """Arrange scores into format that fits consumer logic.

    :param scores: output from prediction
    :return: scores in agreed on format
    """
    scores_output = {}
    for seq, score in enumerate(scores, 1):
        data = {
            "inventory_id": score["id"],
            "recommendations": {
                'depth': score["depth"],
                'is_anomalous': score["is_anomalous"],
                'score': score["score"],
            },
        }
        scores_output[seq] = data
    return scores_output


def compile_charts(charts):
    """Arrange charts into format that fits consumer logic.

    :param scores: output from prediction
    :return: scores in agreed on format
    """
    chart_output = []
    for chart_type, svg in charts.items():
        chart_output.append({
            "chart_type": chart_type,
            "svg_contents": svg,
        })
    return chart_output


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
        scores = compile_scores(result)
        contrasts = isolation_forest.contrast()
        try:
            charts = compile_charts(isolation_forest.to_report())
        except NameError:
            charts = []
            LOGGER.warning("Cannot plot chart without matplotlib")

        LOGGER.info('Analysis have %s rows in scores', len(result))

        # Build response JSON
        output = {
            'id': batch_id,
            'ai_service': env['ai_service'],
            'data': {
                'account_number': account_id,
                'hosts': scores,
                'common_data': [
                    {'contrasts': contrasts},
                    {'charts': charts},
                ]
            }
        }

        url = f'http://{next_service}'
        LOGGER.info(
            '%s: Job ID %s: detection done, publishing to %s ...',
            thread.name, batch_id, url
        )

        # Pass to the next service
        try:
            _retryable(
                'post',
                url,
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
