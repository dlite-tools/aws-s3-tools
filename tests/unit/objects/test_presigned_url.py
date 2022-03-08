"""Unit tests for presigned_url.py"""
import requests

import pytest
from botocore.exceptions import ParamValidationError

from s3_tools import (
    get_presigned_download_url,
    get_presigned_upload_url,
    get_presigned_url,
    object_exists
)

from tests.unit.conftest import (
    BUCKET_NAME,
    FILENAME,
    create_bucket
)


class TestPresignedUrl:
    fn_test = FILENAME + ".tests"
    key = "prefix/object"

    def test_download_nonexisting_object_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(requests.exceptions.HTTPError):
            url = get_presigned_download_url(bucket=BUCKET_NAME, key=self.key)
            response = requests.get(url)
            response.raise_for_status()

            assert response.status_code == 404

    def test_download_object_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.key, FILENAME)]):
            url = get_presigned_download_url(bucket=BUCKET_NAME, key=self.key)
            response = requests.get(url)

        assert url is not None
        assert response.status_code == 200

    def test_list_bucket_objects_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.key, FILENAME)]):
            url = get_presigned_url(client_method='list_objects', method_parameters={'Bucket': BUCKET_NAME})
            response = requests.get(url)

        assert response.status_code == 200

    def test_invalid_request_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(ParamValidationError):
            url = get_presigned_url(client_method='list_objects')
            requests.get(url)

    def test_post_objects_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, self.key)
            response = get_presigned_upload_url(bucket=BUCKET_NAME, key=self.key)
            with open(FILENAME, 'rb') as f:
                files = {'file': (self.key, f)}
                post_response = requests.post(response['url'], data=response['fields'], files=files)

            after = object_exists(BUCKET_NAME, self.key)

        assert post_response.status_code == 204
        assert before is False
        assert after is True

    def test_invalid_post_objects_with_presigned_url(self, s3_client):
        with pytest.raises(AttributeError):
            get_presigned_upload_url(bucket=BUCKET_NAME, key=None)
