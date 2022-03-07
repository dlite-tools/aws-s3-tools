"""Unit tests for create bucket."""
from s3_tools import (
    create_bucket,
    delete_bucket,
)

import pytest
from botocore.exceptions import ParamValidationError


class TestDelete:

    def test_delete_bucket(self, s3_client):

        create_bucket('myBucket')
        response = delete_bucket('myBucket')

        assert response is True

    def test_create_bucket_invalid_name(self, s3_client):

        with pytest.raises(ParamValidationError):
            delete_bucket('')

    def test_delete_nonexisting_bucket(self, s3_client):
        response = delete_bucket('myBucket')

        assert response is False
