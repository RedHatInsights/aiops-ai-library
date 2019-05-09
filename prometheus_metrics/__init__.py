"""Metrics interface."""

from .prometheus_metrics import generate_aggregated_metrics, METRICS

__all__ = ["generate_aggregated_metrics", "METRICS"]
