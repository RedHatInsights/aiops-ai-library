from prometheus_client import (Counter, Histogram, generate_latest,
                               CollectorRegistry, multiprocess)

# Prometheus Metrics
METRICS = {
    'preparation_time': Histogram(
        'aiops_outlier_detection_preparation_seconds',
        'Time spent preparing data for outlier detection'
    ),
    'processing_time': Histogram(
        'aiops_outlier_detection_processing_seconds',
        'Time spent running outlier detection'
    ),
    'report_time': Histogram(
        'aiops_outlier_detection_report_seconds',
        'Time spent creating report for outlier detection results'
    ),
    'request_time': Histogram(
        'aiops_outlier_detection_request_seconds',
        'Time spent for end-to-end outlier detection'
    ),
    'data_size': Histogram(
        'aiops_outlier_detection_data_size',
        'The total number of systems in data set'
    ),
    'feature_size': Histogram(
        'aiops_outlier_detection_feature_size',
        'The number of features participating outlier detection'
    ),
    'jobs_published': Counter(
        'aiops_outlier_detection_jobs_published',
        'The total number of jobs have results published'
    ),
}


def generate_aggregated_metrics():
    """Generate Aggregated Metrics for multiple processes."""
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)
