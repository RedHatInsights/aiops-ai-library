from prometheus_client import (Counter, Histogram, generate_latest,
                               CollectorRegistry, multiprocess)
from prometheus_client.utils import INF

# Prometheus Metrics
METRICS = {
    'preparation_time': Histogram(
        'aiops_outlier_detection_preparation_seconds',
        'Time spent preparing data for outlier detection',
        buckets=(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, INF),
    ),
    'processing_time': Histogram(
        'aiops_outlier_detection_processing_seconds',
        'Time spent running outlier detection',
        buckets=(60, 120, 180, 240, 300, 360, 420, 480, 540, 600, INF),
    ),
    'report_time': Histogram(
        'aiops_outlier_detection_report_seconds',
        'Time spent creating report for outlier detection results',
        buckets=(5, 10, 15, 20, 25, 30, 35, 40, 45, 50, INF),
    ),
    'request_time': Histogram(
        'aiops_outlier_detection_request_seconds',
        'Time spent for end-to-end outlier detection',
        buckets=(60, 120, 180, 240, 300, 360, 420, 480, 540, 600, INF),
    ),
    'data_size': Histogram(
        'aiops_outlier_detection_data_size',
        'The total number of systems in data set',
        buckets=(500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, INF),
    ),
    'feature_size': Histogram(
        'aiops_outlier_detection_feature_size',
        'The number of features participating outlier detection',
        buckets=(100, 1000, 2000, 3000, 4000, 5000, INF),
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
