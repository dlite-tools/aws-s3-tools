"""Unit tests for check.py"""
from s3_tools import object_exists

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME,
    FILENAME
)


class TestUtils:

    def test_check_nonexisting_bucket(self, s3_client):
        assert object_exists(BUCKET_NAME, "prefix/key.csv") is False

    def test_check_nonexisting_object(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            response = object_exists(BUCKET_NAME, "prefix/key.csv")

        assert response is False

    def test_check_existing_object(self, s3_client):
        key = "prefix/key.csv"
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            response = object_exists(BUCKET_NAME, key)

        assert response is True
