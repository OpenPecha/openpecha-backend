# import sys
# from unittest.mock import MagicMock

# import pytest

# @pytest.fixture(autouse=True)
# def mock_google_cloud_logging():
#     """Mock Google Cloud Logging during tests to prevent import errors."""
#     sys.modules["google.cloud.logging"] = MagicMock()
