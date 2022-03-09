"""Unit tests for check bucket."""
import pytest
from botocore.exceptions import ParamValidationError

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

    def test_check_invalid_param(self, s3_client):
        with pytest.raises(ParamValidationError):
            bucket_exists("")
