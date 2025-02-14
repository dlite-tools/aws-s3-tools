"""Unit tests for move module."""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from s3_tools import (
    move_keys,
    move_object,
    object_exists,
)
from tests.unit.conftest import (
    BUCKET_NAME,
    FILENAME,
    create_bucket,
)


class TestMove:

    source_key = "prefix/object"
    destination_key = "new-prefix/new-object"
    destination_bucket = "another-bucket"

    source_keys = [f"prefix/object_0{i}" for i in range(1, 5)]
    destination_keys = [f"new-prefix/new-object_0{i}" for i in range(1, 5)]

    source_keys_path = [Path(key) for key in source_keys]
    destination_keys_path = [Path(key) for key in destination_keys]

    def test_move_from_nonexisting_bucket(self, s3_client):
        try:
            move_object("Bucket", "key", "Bucket", "new-key")
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_move_from_nonexisting_key(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            try:
                move_object(BUCKET_NAME, "key", BUCKET_NAME, "new-key")
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "404"

    @pytest.mark.parametrize("source,destination", [
        (source_key, destination_key),
        (source_key, Path(destination_key)),
        (Path(source_key), destination_key),
        (Path(source_key), Path(destination_key)),
    ])
    def test_move_inside_bucket(self, s3_client, source, destination):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(source, FILENAME)]):
            source_before = object_exists(BUCKET_NAME, source)
            dest_before = object_exists(BUCKET_NAME, destination)

            move_object(BUCKET_NAME, source, BUCKET_NAME, destination)

            source_after = object_exists(BUCKET_NAME, source)
            dest_after = object_exists(BUCKET_NAME, destination)

        assert source_before is True and dest_before is False
        assert source_after is False and dest_after is True

    @pytest.mark.parametrize("source,destination", [
        (source_key, destination_key),
        (source_key, Path(destination_key)),
        (Path(source_key), destination_key),
        (Path(source_key), Path(destination_key)),
    ])
    def test_move_between_bucket(self, s3_client, source, destination):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(source, FILENAME)]), \
                create_bucket(s3_client, self.destination_bucket):

            source_before = object_exists(BUCKET_NAME, source)
            dest_before = object_exists(self.destination_bucket, destination)

            move_object(BUCKET_NAME, source, self.destination_bucket, destination)

            source_after = object_exists(BUCKET_NAME, source)
            dest_after = object_exists(self.destination_bucket, destination)

        assert source_before is True and dest_before is False
        assert source_after is False and dest_after is True

    def test_move_to_nonexisting_bucket(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.source_key, FILENAME)]):
            try:
                move_object(BUCKET_NAME, self.source_key, self.destination_bucket, self.destination_key)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_move_list_length_zero(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(ValueError):
            move_keys(BUCKET_NAME, [], self.destination_bucket, [])

    def test_move_list_different_length(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(IndexError):
            move_keys(BUCKET_NAME, [self.source_key], self.destination_bucket, [])

    @pytest.mark.parametrize("source,destination", [
        (source_keys, destination_keys),
        (source_keys, destination_keys_path),
        (source_keys_path, destination_keys),
        (source_keys_path, destination_keys_path),
    ])
    def test_move_list_inside_bucket(self, s3_client, source, destination):
        keys_paths = [(key, FILENAME) for key in source]
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=keys_paths):
            source_before = [object_exists(BUCKET_NAME, key) for key in source]
            dest_before = [object_exists(BUCKET_NAME, key) for key in destination]

            move_keys(BUCKET_NAME, source, BUCKET_NAME, destination)

            source_after = [object_exists(BUCKET_NAME, key) for key in source]
            dest_after = [object_exists(BUCKET_NAME, key) for key in destination]

            assert all(source_before) is True and all(dest_before) is False
            assert all(source_after) is False and all(dest_after) is True
