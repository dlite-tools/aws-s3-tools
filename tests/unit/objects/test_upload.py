"""Unit tests for upload.py"""
import shutil

import pytest
from boto3.exceptions import S3UploadFailedError

from s3_tools import (
    upload_file_to_key,
    upload_files_to_keys,
    upload_folder_to_prefix,
    object_exists
)

from tests.unit.conftest import (
    BUCKET_NAME,
    EMPTY_FILE,
    FILENAME,
    create_bucket,
    create_files
)


class TestUpload:
    key = "prefix/object"

    def test_upload_nonexisting_bucket(self, s3_client):
        with pytest.raises(S3UploadFailedError):
            upload_file_to_key(BUCKET_NAME, self.key, FILENAME)

    def test_upload_nonexisting_file(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            with pytest.raises(FileNotFoundError):
                upload_file_to_key(BUCKET_NAME, self.key, "/tmp/nonexisting.file")

    def test_upload_file(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, self.key)
            upload_file_to_key(BUCKET_NAME, self.key, FILENAME)
            after = object_exists(BUCKET_NAME, self.key)

        assert before is False
        assert after is True

    def test_upload_empty_file(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, self.key)
            upload_file_to_key(BUCKET_NAME, self.key, EMPTY_FILE)
            after = object_exists(BUCKET_NAME, self.key)

        assert before is False
        assert after is True

    @pytest.mark.parametrize('show', [False, True])
    def test_upload_files_to_keys(self, s3_client, show):
        if show:
            pytest.importorskip("rich")

        lst = [(FILENAME, f"prefix/mock_{i}.csv") for i in range(4)]
        with create_bucket(s3_client, BUCKET_NAME):
            before = [object_exists(BUCKET_NAME, key) for fn, key in lst]
            upload_files_to_keys(BUCKET_NAME, lst, show_progress=show)
            after = [object_exists(BUCKET_NAME, key) for fn, key in lst]

        assert all(before) is False
        assert all(after) is True

    def test_upload_folder_to_prefix(self, s3_client):
        root_folder = 'TEST_ROOT_A'
        structure = {
            "file.root": "This file is in the root folder",
            "folderA": {
                "file.A1": "This file is in the folder A - file A1",
                "file.A2": "This file is in the folder A - file A2"
            },
            "folderB": {},
            "folderC": {
                "folderD": {
                    "file.D1": "This file is in the folder D - file D1"
                }
            }
        }
        paths = create_files(root_folder, structure)

        with create_bucket(s3_client, BUCKET_NAME):
            response = upload_folder_to_prefix(BUCKET_NAME, "prefix", root_folder)

        shutil.rmtree(root_folder)

        assert len(response) == 4
        # The response must content all paths
        assert not set(paths) ^ set(r[0] for r in response)
