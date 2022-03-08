"""Unit tests for check bucket."""
from s3_tools import bucket_exists

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME
)


class TestCheck:

    def test_check_nonexisting_bucket(self, s3_client):
        response = bucket_exists(BUCKET_NAME)

        assert response is False

    def test_check_existing_bucket(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            response = bucket_exists(BUCKET_NAME)

        assert response is True
