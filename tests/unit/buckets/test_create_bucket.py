"""Unit tests for create bucket."""
import pytest
from botocore.exceptions import ParamValidationError

from s3_tools import create_bucket
from tests.unit.conftest import BUCKET_NAME


class TestCreate:

    def test_create_bucket(self, s3_client):
        response = create_bucket(BUCKET_NAME)

        assert response is True

    def test_create_bucket_invalid_name(self, s3_client):

        with pytest.raises(ParamValidationError):
            create_bucket('')

    def test_create_duplicated_bucket(self, s3_client):
        create_bucket(BUCKET_NAME)

        response = create_bucket(BUCKET_NAME)

        assert response is True
