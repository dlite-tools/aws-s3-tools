"""Unit tests for list.py"""
from pathlib import Path

from botocore.exceptions import ClientError
import pytest

from s3_tools import list_objects

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME,
    FILENAME
)


class TestList:

    def test_list_nonexisting_bucket(self, s3_client):
        try:
            list_objects(BUCKET_NAME)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    @pytest.mark.parametrize("prefix", ["prefix", Path("prefix")])
    def test_list_empty_bucket(self, s3_client, prefix):
        with create_bucket(s3_client, BUCKET_NAME):
            keys = list_objects(BUCKET_NAME, prefix)

        assert len(keys) == 0

    def test_list_bucket(self, s3_client):
        lst = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(1)]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=lst):
            keys = list_objects(BUCKET_NAME, "prefix")

        assert len(keys) == 1
        assert keys[0] == lst[0][0]

    def test_list_bucket_with_pagination(self, s3_client):
        lst = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(10)]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=lst):
            keys = list_objects(BUCKET_NAME, "prefix", max_keys=3)

        assert len(keys) == 10

    def test_list_bucket_return_as_path(self, s3_client):
        lst = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(1)]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=lst):
            keys = list_objects(BUCKET_NAME, "prefix", as_paths=True)

        assert len(keys) == 1
        assert keys[0] == Path(lst[0][0])
