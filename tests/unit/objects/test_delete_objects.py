"""Unit tests for delete.py"""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from s3_tools import (
    delete_keys,
    delete_object,
    delete_prefix,
    object_exists
)

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME,
    FILENAME
)


class TestDelete:

    files = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(4)]
    keys = [key for key, fn in files]
    keys_path = [Path(key) for key in keys]

    def test_delete_nonexisting_bucket(self, s3_client):
        try:
            delete_object(BUCKET_NAME, "prefix/object")
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    @pytest.mark.parametrize("key", ["prefix/object", Path("prefix/object")])
    def test_delete_nonexisting_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, key)
            delete_object(BUCKET_NAME, key)
            after = object_exists(BUCKET_NAME, key)

        assert before is False
        assert after is False

    @pytest.mark.parametrize("key", ["prefix/object", Path("prefix/object")])
    def test_delete_existing_object(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            before = object_exists(BUCKET_NAME, key)
            delete_object(BUCKET_NAME, key)
            after = object_exists(BUCKET_NAME, key)

        assert before is True
        assert after is False

    @pytest.mark.parametrize("keys", [keys, keys_path])
    def test_delete_keys(self, s3_client, keys):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=self.files):
            before = [object_exists(BUCKET_NAME, key) for key in keys]
            delete_keys(BUCKET_NAME, keys, False)
            after = [object_exists(BUCKET_NAME, key) for key in keys]

        assert all(before) is True
        assert all(after) is False

    @pytest.mark.parametrize("keys", [keys, keys_path])
    def test_delete_prefix(self, s3_client, keys):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=self.files):
            before = [object_exists(BUCKET_NAME, key) for key in keys]
            delete_prefix(BUCKET_NAME, "prefix", False)
            after = [object_exists(BUCKET_NAME, key) for key in keys]

        assert all(before) is True
        assert all(after) is False

    @pytest.mark.parametrize("keys", [keys, keys_path])
    def test_delete_prefix_dry_run(self, s3_client, keys):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=self.files):
            before = [object_exists(BUCKET_NAME, key) for key in keys]
            delete_prefix(BUCKET_NAME, "prefix", True)
            after = [object_exists(BUCKET_NAME, key) for key in keys]

        assert all(before) is True
        assert all(after) is True

    @pytest.mark.parametrize("keys", [keys, keys_path])
    def test_delete_keys_dry_run(self, s3_client, keys):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=self.files):
            before = [object_exists(BUCKET_NAME, key) for key in keys]
            delete_keys(BUCKET_NAME, keys, True)
            after = [object_exists(BUCKET_NAME, key) for key in keys]

        assert all(before) is True
        assert all(after) is True
