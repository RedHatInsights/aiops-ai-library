from imp import reload
import pytest

import server


@pytest.fixture(autouse=True)
def patch_env(monkeypatch):
    """Set up environment."""
    # Set Test variables
    monkeypatch.setenv('NEXT_SERVICE_URL', 'http://publisher:8080')

    reload(server)
