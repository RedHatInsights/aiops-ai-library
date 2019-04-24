import workers

# R0201 = Method could be a function Used when a method doesn't use its bound
# instance, and so could be written as a function.
# pylint: disable=R0201


def test_isolation_forest_params():
    """Parameters are within bounds."""
    data_size = 10
    trees, sample_size = workers.isolation_forest_params(0, 0, data_size)
    assert 0 < trees < data_size
    assert 0 < sample_size < data_size

    trees, sample_size = workers.isolation_forest_params(1, 1.1, data_size)
    assert 0 < trees < data_size
    assert 0 < sample_size < data_size
