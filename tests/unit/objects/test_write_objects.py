"""Unit tests for write module."""
import pytest
from botocore.exceptions import ClientError
from s3_tools import object_exists, write_object_from_bytes, write_object_from_dict, write_object_from_text
from tests.unit.conftest import BUCKET_NAME, create_bucket


class TestWrite:
    key = "prefix/object"

    def test_write_nonexisting_bucket(self, s3_client):
        obj = bytes("Just a test string converted to bytes", 'utf-8')
        try:
            write_object_from_bytes(BUCKET_NAME, self.key, obj)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_write_from_empty_bytes(self, s3_client):
        obj = bytes()

        with create_bucket(s3_client, BUCKET_NAME):
            url = write_object_from_bytes(BUCKET_NAME, self.key, obj)
            exists = object_exists(BUCKET_NAME, self.key)

        assert url == f"https://s3.amazonaws.com/{BUCKET_NAME}/{self.key}"
        assert exists is True

    def test_write_from_bytes(self, s3_client):
        obj = bytes("Just a test string converted to bytes", 'utf-8')

        with create_bucket(s3_client, BUCKET_NAME):
            url = write_object_from_bytes(BUCKET_NAME, self.key, obj)
            exists = object_exists(BUCKET_NAME, self.key)

        assert url == f"https://s3.amazonaws.com/{BUCKET_NAME}/{self.key}"
        assert exists is True

    def test_write_from_bytes_wrong_format(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            with pytest.raises(TypeError):
                write_object_from_bytes(BUCKET_NAME, self.key, 10)  # type: ignore

    def test_write_from_dict(self, s3_client):
        obj = {"key": "value"}

        with create_bucket(s3_client, BUCKET_NAME):
            url = write_object_from_dict(BUCKET_NAME, self.key, obj)
            exists = object_exists(BUCKET_NAME, self.key)

        assert url == f"https://s3.amazonaws.com/{BUCKET_NAME}/{self.key}"
        assert exists is True

    def test_write_from_dict_wrong_format(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            with pytest.raises(TypeError):
                write_object_from_dict(BUCKET_NAME, self.key, 10)  # type: ignore

    def test_write_from_text(self, s3_client):
        obj = "Just a test string"

        with create_bucket(s3_client, BUCKET_NAME):
            url = write_object_from_text(BUCKET_NAME, self.key, obj)
            exists = object_exists(BUCKET_NAME, self.key)

        assert url == f"https://s3.amazonaws.com/{BUCKET_NAME}/{self.key}"
        assert exists is True

    def test_write_from_text_wrong_format(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            with pytest.raises(TypeError):
                write_object_from_text(BUCKET_NAME, self.key, 10)  # type: ignore
