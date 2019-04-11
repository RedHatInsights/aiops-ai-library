import json

import workers

# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.
# pylint: disable=R0201


class TestRadOutput:
    """Test. Output for consumer logic."""

    def test_compile_scores(self):
        """Output scores as agreed on."""
        results = [
            {
                'depth': 2.9380611476541687,
                'id': '18d00a23-dd63-41b0-8617-7f58b56dc71e',
                'is_anomalous': True,
                'score': 0.5979060442543648
            },
            {
                'depth': 4.604944571625036,
                'id': '59203d52-65a6-4b89-be40-f4f557136208',
                'is_anomalous': False,
                'score': 0.44658855423307714
            },
        ]
        host_by_id = {
            '18d00a23-dd63-41b0-8617-7f58b56dc71e': {
                'display_name': 'hello'
            },
            '59203d52-65a6-4b89-be40-f4f557136208': {
                'display_name': 'world'
            }
        }
        scores = workers.compile_scores(results, host_by_id)
        expected = {
            "1": {
                "inventory_id": '18d00a23-dd63-41b0-8617-7f58b56dc71e',
                "recommendations": {
                    'depth': 2.9380611476541687,
                    'is_anomalous': True,
                    'score': 0.5979060442543648,
                    'display_name': 'hello',
                },
            },
            "2": {
                "inventory_id": '59203d52-65a6-4b89-be40-f4f557136208',
                "recommendations": {
                    'depth': 4.604944571625036,
                    'is_anomalous': False,
                    'score': 0.44658855423307714,
                    'display_name': 'world'
                },
            },
        }
        scores_dump = json.dumps(scores, sort_keys=True, indent=2)
        expected_dump = json.dumps(expected, sort_keys=True, indent=2)
        assert scores_dump == expected_dump

    def test_compile_charts(self):
        """Output charts as agreed on."""
        results = {
            "boxplot": "some svg string1",
            "hist": "some svg string2",
        }
        charts = workers.compile_charts(results)
        expected = [
            {
                "chart_type": "boxplot",
                "svg_contents": "some svg string1",
            },
            {
                "chart_type": "hist",
                "svg_contents": "some svg string2",
            }
        ]
        charts_dump = json.dumps(charts, sort_keys=True, indent=2)
        expected_dump = json.dumps(expected, sort_keys=True, indent=2)
        assert charts_dump == expected_dump

    def test_isolation_forest_params(self):
        """Parameters are within bounds."""
        data_size = 10
        trees, sample_size = workers.isolation_forest_params(0, 0, data_size)
        assert 0 < trees < data_size
        assert 0 < sample_size < data_size

        trees, sample_size = workers.isolation_forest_params(1, 1.1, data_size)
        assert 0 < trees < data_size
        assert 0 < sample_size < data_size
