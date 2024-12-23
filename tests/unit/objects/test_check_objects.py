"""Unit tests for check module."""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from s3_tools import (
    object_exists,
    object_metadata,
)
from tests.unit.conftest import BUCKET_NAME, FILENAME, create_bucket


class TestCheck:

    def test_check_nonexisting_bucket(self, s3_client):
        with pytest.raises(ClientError):
            object_exists(BUCKET_NAME, "prefix/key.csv")

    @pytest.mark.parametrize("key", ["prefix/key.csv", Path("prefix/key.csv/")])
    def test_check_nonexisting_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            response = object_exists(BUCKET_NAME, key)

        assert response is False

    @pytest.mark.parametrize("key", ["prefix/key.csv", Path("prefix/key.csv/")])
    def test_check_existing_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            response = object_exists(BUCKET_NAME, key)

        assert response is True


class TestMetadata:

    def test_metadata_nonexisting_bucket(self, s3_client):
        with pytest.raises(ClientError):
            object_metadata(BUCKET_NAME, "prefix/key.csv")

    @pytest.mark.parametrize("key", ["prefix/key.csv", Path("prefix/key.csv/")])
    def test_metadata_nonexisting_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            response = object_metadata(BUCKET_NAME, key)

        assert response == {}

    @pytest.mark.parametrize("key", ["prefix/key.csv", Path("prefix/key.csv/")])
    def test_metadata_existing_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            response = object_metadata(BUCKET_NAME, key)

        assert response['ContentLength'] == 79
        assert response['ContentType'] == 'binary/octet-stream'
