"""Unit tests for presigned_url module."""
from pathlib import Path

import pytest
import requests
from botocore.exceptions import ParamValidationError
from s3_tools import (
    get_presigned_download_url,
    get_presigned_upload_url,
    get_presigned_url,
    object_exists,
)
from tests.unit.conftest import (
    BUCKET_NAME,
    FILENAME,
    create_bucket,
)


class TestPresignedUrl:
    fn_test = FILENAME + ".tests"
    key = "prefix/object"

    def test_download_nonexisting_object_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(requests.exceptions.HTTPError):
            url = get_presigned_download_url(bucket=BUCKET_NAME, key=self.key)
            response = requests.get(url)
            response.raise_for_status()

            assert url is not None
            assert response.status_code == 404

    @pytest.mark.parametrize("key", [key, Path(key)])
    def test_download_object_with_presigned_url(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            url = get_presigned_download_url(bucket=BUCKET_NAME, key=key)
            response = requests.get(url)

        assert url is not None
        assert response.status_code == 200

    def test_list_bucket_objects_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.key, FILENAME)]):
            url = get_presigned_url(client_method='list_objects', method_parameters={'Bucket': BUCKET_NAME})
            response = requests.get(url)

        assert url is not None
        assert response.status_code == 200

    def test_invalid_request_with_presigned_url(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME), pytest.raises(ParamValidationError):
            url = get_presigned_url(client_method='list_objects')
            requests.get(url)

    @pytest.mark.parametrize("key", [key, Path(key)])
    def test_upload_objects_with_presigned_url(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, self.key)
            response = get_presigned_upload_url(bucket=BUCKET_NAME, key=key)
            print(response)
            with open(FILENAME, 'rb') as f:
                files = {'file': (Path(key).as_posix(), f)}
                post_response = requests.post(response['url'], data=response['fields'], files=files)

            after = object_exists(BUCKET_NAME, key)

        assert response is not None
        assert post_response.status_code == 204
        assert before is False
        assert after is True

    @pytest.mark.parametrize("key", [None, int])
    def test_invalid_upload_objects_with_presigned_url(self, s3_client, key):
        with pytest.raises((AttributeError, TypeError)):
            get_presigned_upload_url(bucket=BUCKET_NAME, key=key)
