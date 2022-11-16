"""Unit tests for upload.py"""
from pathlib import Path
import shutil

import pytest
from boto3.exceptions import S3UploadFailedError

from s3_tools import (
    upload_file_to_key,
    upload_files_to_keys,
    upload_folder_to_prefix,
    object_exists,
    object_metadata,
)

from tests.unit.conftest import (
    BUCKET_NAME,
    EMPTY_FILE,
    FILENAME,
    create_bucket,
    create_files,
)


class TestUpload:
    key = "prefix/object"
    root_folder = 'TEST_ROOT_A'

    keys = [(FILENAME, f"prefix/mock_{i}.csv") for i in range(4)]
    keys_paths = [(Path(fn), Path(key)) for fn, key in keys]

    def test_upload_nonexisting_bucket(self, s3_client):
        with pytest.raises(S3UploadFailedError):
            upload_file_to_key(BUCKET_NAME, self.key, FILENAME)

    def test_upload_nonexisting_file(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            with pytest.raises(FileNotFoundError):
                upload_file_to_key(BUCKET_NAME, self.key, "/tmp/nonexisting.file")

    @pytest.mark.parametrize("key", [key, Path(key)])
    def test_upload_file(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, key)
            upload_file_to_key(BUCKET_NAME, key, FILENAME)
            after = object_exists(BUCKET_NAME, key)

        assert before is False
        assert after is True

    @pytest.mark.parametrize("key", [key, Path(key)])
    def test_upload_empty_file(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, key)
            upload_file_to_key(BUCKET_NAME, key, EMPTY_FILE)
            after = object_exists(BUCKET_NAME, key)

        assert before is False
        assert after is True

    @pytest.mark.parametrize('keys,show', [
        (keys, False),
        (keys_paths, True),
    ])
    def test_upload_files_to_keys(self, s3_client, keys, show):
        if show:
            pytest.importorskip("rich")

        with create_bucket(s3_client, BUCKET_NAME):
            before = [object_exists(BUCKET_NAME, key) for fn, key in keys]
            upload_files_to_keys(BUCKET_NAME, keys, show_progress=show)
            after = [object_exists(BUCKET_NAME, key) for fn, key in keys]

        assert all(before) is False
        assert all(after) is True

    @pytest.mark.parametrize('prefix,as_path', [
        ("prefix", False),
        (Path("prefix"), True),
    ])
    def test_upload_folder_to_prefix(self, s3_client, prefix, as_path):
        paths = create_files(as_path)

        with create_bucket(s3_client, BUCKET_NAME):
            response = upload_folder_to_prefix(BUCKET_NAME, prefix, self.root_folder, as_paths=as_path)

        shutil.rmtree(self.root_folder)

        assert len(response) == 4
        # The response must content all paths
        assert not set(paths) ^ set(r[0] for r in response)

    def test_upload_not_enough_arguments(self):

        with pytest.raises(ValueError):
            upload_files_to_keys(BUCKET_NAME, self.keys_paths, extra_args_per_key=[{'arg': 'value'}])

    @pytest.mark.parametrize("key", [key, Path(key)])
    def test_update_with_arguments(self, s3_client, key):
        with create_bucket(s3_client, BUCKET_NAME):
            before = object_exists(BUCKET_NAME, key)
            upload_file_to_key(
                BUCKET_NAME,
                key,
                FILENAME,
                extra_args={
                    'Metadata': {'key': 'valueA'},
                    'ContentType': 'text/csv',
                    'Tagging': 'tagA=valueA&tagB=valueB',
                }
            )
            after = object_exists(BUCKET_NAME, key)
            metadata = object_metadata(BUCKET_NAME, key)

        assert before is False
        assert after is True

        assert metadata['Metadata']['key'] == 'valueA'
        assert metadata['ContentType'] == 'text/csv'
        assert metadata['ResponseMetadata']['HTTPHeaders']['x-amz-tagging-count'] == '2'
