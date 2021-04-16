"""Unit tests for download.py"""
import shutil
from filecmp import dircmp
from pathlib import Path

from botocore.exceptions import ClientError

from s3_tools import (
    download_key_to_file,
    download_keys_to_files,
    download_prefix_to_folder
)

from tests.unit.conftest import (
    BUCKET_NAME,
    EMPTY_FILE,
    FILENAME,
    create_bucket,
    create_files
)


class TestDownload:
    fn_test = FILENAME + ".tests"
    key = "prefix/object"

    def test_download_nonexisting_bucket(self, s3_client):
        try:
            download_key_to_file(BUCKET_NAME, self.key, self.fn_test)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    def test_download_nonexisting_object(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME):
            try:
                download_key_to_file(BUCKET_NAME, self.key, self.fn_test)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "404"

    def test_download_object(self, s3_client):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.key, FILENAME)]):
            before = Path(self.fn_test).exists()
            response = download_key_to_file(BUCKET_NAME, self.key, self.fn_test)
            after = Path(self.fn_test).exists()

        Path(self.fn_test).unlink()
        assert before is False
        assert after is True
        assert response is True

    def test_download_empty_object(self, s3_client):
        empty = EMPTY_FILE + ".tests"
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(self.key, EMPTY_FILE)]):
            before = Path(empty).exists()
            response = download_key_to_file(BUCKET_NAME, self.key, empty)
            after = Path(empty).exists()

        Path(empty).unlink()
        assert before is False
        assert after is True
        assert response is True

    def test_download_keys_to_files(self, s3_client):
        create = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(4)]
        download = [(f"prefix/mock_{i}.csv", f"{FILENAME}.{i}") for i in range(4)]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=create):
            before = [Path(fn).exists() for key, fn in download]
            response = download_keys_to_files(BUCKET_NAME, keys_paths=download)
            after = [Path(fn).exists() for key, fn in download]

        for key, fn in download:
            Path(fn).unlink()

        assert all(before) is False
        assert all(after) is True
        assert all(r[2] for r in response) is True

    def test_download_prefix_to_folder(self, s3_client):
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
        lst = [(p.replace(root_folder, "test_prefix"), p) for p in paths]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=lst):
            response = download_prefix_to_folder(BUCKET_NAME, "test_prefix", "test_folder")

        result = dircmp(root_folder, "test_folder")

        # "Folder B" exists only on root_folder (empty folder is not upload to S3)
        test_1_Root = len(result.left_only) == 1 and len(result.right_only) == 0

        # Both root folders have 1 file and 2 dirs in common
        test_2_Root = len(result.common) == 3 and len(result.diff_files) == 0

        # Both "Folder A" folders have 2 files and 0 dirs in common
        folderA = result.subdirs['folderA']
        test_1_FolderA = len(folderA.common) == 2 and len(folderA.diff_files) == 0

        # Both "Folder D" folders have 1 file and 0 dirs in common
        folderD = result.subdirs['folderC'].subdirs['folderD']
        test_1_FolderD = len(folderD.common) == 1 and len(folderD.diff_files) == 0

        shutil.rmtree(root_folder)
        shutil.rmtree("test_folder")

        assert test_1_Root and test_2_Root and test_1_FolderA and test_1_FolderD
        assert len(response) == 4
