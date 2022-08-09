"""Unit tests for copy.py"""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from s3_tools import (
    copy_keys,
    copy_object,
    copy_prefix,
    object_exists,
    list_objects,
)

from tests.unit.conftest import (
    create_bucket,
    BUCKET_NAME,
    FILENAME
)


class TestCopy:

    source_key = "prefix/object"
    destination_key = "new-prefix/new-object"
    destination_bucket = "another-bucket"

    source_keys = [f"prefix/object_0{i}" for i in range(1, 5)]
    destination_keys = [f"new-prefix/new-object_0{i}" for i in range(1, 5)]

    source_keys_path = [Path(key) for key in source_keys]
    destination_keys_path = [Path(key) for key in destination_keys]

    def test_copy_from_nonexisting_bucket(self, s3_client):
        try:
            copy_object("Bucket", self.source_key, "Bucket", self.destination_key)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    @pytest.mark.parametrize("source,destination", [
        (source_key, destination_key),
        (Path(source_key), Path(destination_key)),
        (source_key, Path(destination_key)),
        (Path(source_key), destination_key),
    ])
    def test_copy_from_nonexisting_key(self, s3_client, source, destination):
        with create_bucket(s3_client, BUCKET_NAME):
            try:
                copy_object(BUCKET_NAME, source, BUCKET_NAME, destination)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "404"

    @pytest.mark.parametrize("source,destination", [
        (source_key, destination_key),
        (Path(source_key), Path(destination_key)),
        (source_key, Path(destination_key)),
        (Path(source_key), destination_key),
    ])
    def test_copy_inside_bucket(self, s3_client, source, destination):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.source_key, FILENAME)]):
            source_before = object_exists(BUCKET_NAME, source)
            dest_before = object_exists(BUCKET_NAME, destination)

            copy_object(BUCKET_NAME, source, BUCKET_NAME, destination)

            source_after = object_exists(BUCKET_NAME, source)
            dest_after = object_exists(BUCKET_NAME, destination)

        assert source_before is True and dest_before is False
        assert source_after is True and dest_after is True

    @pytest.mark.parametrize("source,destination", [
        (source_key, destination_key),
        (Path(source_key), Path(destination_key)),
        (source_key, Path(destination_key)),
        (Path(source_key), destination_key),
    ])
    def test_copy_between_bucket(self, s3_client, source, destination):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.source_key, FILENAME)]), \
                create_bucket(s3_client, self.destination_bucket):

            source_before = object_exists(BUCKET_NAME, source)
            dest_before = object_exists(self.destination_bucket, destination)

            copy_object(BUCKET_NAME, source, self.destination_bucket, destination)

            source_after = object_exists(BUCKET_NAME, source)
            dest_after = object_exists(self.destination_bucket, destination)

        assert source_before is True and dest_before is False
        assert source_after is True and dest_after is True

    def test_copy_to_nonexisting_bucket(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.source_key, FILENAME)]):
            try:
                copy_object(BUCKET_NAME, self.source_key, self.destination_bucket, self.destination_key)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_copy_list_length_zero(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(ValueError):
            copy_keys(BUCKET_NAME, [], self.destination_bucket, [])

    def test_copy_list_different_length(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(IndexError):
            copy_keys(BUCKET_NAME, [self.source_key], self.destination_bucket, [])

    @pytest.mark.parametrize("source,destination", [
        (source_keys, destination_keys),
        (source_keys_path, destination_keys_path),
        (source_keys, destination_keys_path),
        (source_keys_path, destination_keys),
    ])
    def test_copy_list_inside_bucket(self, s3_client, source, destination):
        keys_paths = [(key, FILENAME) for key in source]
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=keys_paths):
            source_before = [object_exists(BUCKET_NAME, key) for key in source]
            dest_before = [object_exists(BUCKET_NAME, key) for key in destination]

            copy_keys(BUCKET_NAME, source, BUCKET_NAME, destination)

            source_after = [object_exists(BUCKET_NAME, key) for key in source]
            dest_after = [object_exists(BUCKET_NAME, key) for key in destination]

            assert all(source_before) is True and all(dest_before) is False
            assert all(source_after) is True and all(dest_after) is True

    @pytest.mark.parametrize("source,prefix", [
        (source_keys, "prefix"),
        (source_keys_path, Path("prefix")),
        (source_keys, Path("prefix")),
        (source_keys_path, "prefix"),
    ])
    def test_copy_prefix_without_replacement(self, s3_client, source, prefix):
        keys_paths = [(key, FILENAME) for key in source]
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=keys_paths), \
                create_bucket(s3_client, self.destination_bucket):

            source_before = [object_exists(BUCKET_NAME, key) for key in source]
            dest_before = list_objects(self.destination_bucket, prefix=prefix)

            copy_prefix(BUCKET_NAME, prefix, self.destination_bucket, filter_keys='*02*')

            source_after = [object_exists(BUCKET_NAME, key) for key in source]
            dest_after = list_objects(self.destination_bucket, prefix=prefix)

            assert all(source_before) is True and all(source_after) is True
            assert len(dest_before) == 0 and len(dest_after) == 1

    @pytest.mark.parametrize("source,prefix,new_prefix", [
        (source_keys, "prefix", "files"),
        (source_keys, Path("prefix"), "files"),
        (source_keys, "prefix", Path("files")),
        (source_keys_path, Path("prefix"), Path("files")),
        (source_keys_path, "prefix", Path("files")),
        (source_keys_path, Path("prefix"), "files"),
    ])
    def test_copy_prefix_with_replacement(self, s3_client, source, prefix, new_prefix):
        keys_paths = [(key, FILENAME) for key in source]
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=keys_paths), \
                create_bucket(s3_client, self.destination_bucket):

            source_before = [object_exists(BUCKET_NAME, key) for key in source]
            dest_before = list_objects(self.destination_bucket, prefix=new_prefix)

            copy_prefix(BUCKET_NAME, prefix, self.destination_bucket, (prefix, new_prefix), '*')

            source_after = [object_exists(BUCKET_NAME, key) for key in source]
            dest_after_new_prefix = list_objects(self.destination_bucket, prefix=new_prefix)
            dest_after_old_prefix = list_objects(self.destination_bucket, prefix=prefix)

            assert all(source_before) is True and all(source_after) is True
            assert len(dest_before) == 0 and len(dest_after_old_prefix) == 0 and len(dest_after_new_prefix) == 4
