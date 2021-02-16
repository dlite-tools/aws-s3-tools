"""Unit tests for read.py"""
import json

from botocore.exceptions import ClientError

from s3_tools import (
    read_object_to_bytes,
    read_object_to_dict,
    read_object_to_text
)

from tests.unit.conftest import (
    BUCKET_NAME,
    create_bucket
)


class TestRead:
    key = "prefix/object"

    def test_read_nonexisting_bucket(self, s3_client):
        try:
            read_object_to_bytes(BUCKET_NAME, self.key)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_read_nonexisting_object(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            try:
                read_object_to_bytes(BUCKET_NAME, self.key)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "NoSuchKey"

    def test_read_to_bytes(self, s3_client):
        expected_obj = bytes("Just a test string converted to bytes", "utf-8")

        with create_bucket(s3_client, BUCKET_NAME, key=self.key, data=expected_obj):
            obj = read_object_to_bytes(BUCKET_NAME, self.key)

        assert expected_obj == obj

    def test_read_to_dict(self, s3_client):
        expected_obj = {"key": "value"}

        with create_bucket(s3_client, BUCKET_NAME, key=self.key, data=json.dumps(expected_obj)):
            obj = read_object_to_dict(BUCKET_NAME, self.key)

        assert expected_obj == obj

    def test_read_to_text(self, s3_client):
        expected_obj = "Just a test string"

        with create_bucket(s3_client, BUCKET_NAME, key=self.key, data=expected_obj):
            obj = read_object_to_text(BUCKET_NAME, self.key)

        assert expected_obj == obj
