"""Unit tests for download.py"""
import shutil
from filecmp import dircmp
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from s3_tools import (
    download_key_to_file,
    download_keys_to_files,
    download_prefix_to_folder,
)

from tests.unit.conftest import (
    BUCKET_NAME,
    EMPTY_FILE,
    FILENAME,
    create_bucket,
    create_files,
)


class TestDownload:
    fn_test = FILENAME + ".tests"
    key = "prefix/object"

    create = [(f"prefix/mock_{i}.csv", FILENAME) for i in range(4)]
    create_path = [(Path(key), Path(fn)) for key, fn in create]

    download = [(f"prefix/mock_{i}.csv", f"{FILENAME}.{i}") for i in range(4)]
    download_path = [(Path(key), Path(fn)) for key, fn in download]

    root_folder = "TEST_ROOT_A"

    def test_download_nonexisting_bucket(self, s3_client):
        try:
            download_key_to_file(BUCKET_NAME, self.key, self.fn_test)
        except ClientError as e:
            error = e.response["Error"]["Code"]

        assert error == "NoSuchBucket"

    @pytest.mark.parametrize("key,fn", [
        (key, fn_test),
        (key, Path(fn_test)),
        (Path(key), fn_test),
        (Path(key), Path(fn_test)),
    ])
    def test_download_nonexisting_object(self, s3_client, key, fn):
        with create_bucket(s3_client, BUCKET_NAME):
            try:
                download_key_to_file(BUCKET_NAME, key, fn)
            except ClientError as e:
                error = e.response["Error"]["Code"]

        assert error == "404"

    @pytest.mark.parametrize("key,fn", [
        (key, fn_test),
        (key, Path(fn_test)),
        (Path(key), fn_test),
        (Path(key), Path(fn_test)),
    ])
    def test_download_object(self, s3_client, key, fn):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, FILENAME)]):
            before = Path(fn).exists()
            response = download_key_to_file(BUCKET_NAME, key, fn)
            after = Path(fn).exists()

        Path(fn).unlink()
        assert before is False
        assert after is True
        assert response is True

    @pytest.mark.parametrize("key,fn", [
        (key, fn_test),
        (key, Path(fn_test)),
        (Path(key), fn_test),
        (Path(key), Path(fn_test)),
    ])
    def test_download_empty_object(self, s3_client, key, fn):
        with create_bucket(s3_client, BUCKET_NAME, keys_paths=[(key, EMPTY_FILE)]):
            before = Path(fn).exists()
            response = download_key_to_file(BUCKET_NAME, key, fn)
            after = Path(fn).exists()

        Path(fn).unlink()
        assert before is False
        assert after is True
        assert response is True

    @pytest.mark.parametrize('show,create,download,as_paths', [
        (False, create, download, True),
        (False, create, download_path, True),
        (True, create_path, download, False),
        (True, create_path, download_path, False),
    ])
    def test_download_keys_to_files(self, s3_client, show, create, download, as_paths):
        if show:
            pytest.importorskip("rich")

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=create):
            before = [Path(fn).exists() for key, fn in download]
            response = download_keys_to_files(
                bucket=BUCKET_NAME,
                keys_paths=download,
                show_progress=show,
                as_paths=as_paths,
            )
            after = [Path(fn).exists() for key, fn in download]

        for key, fn in download:
            Path(fn).unlink()

        assert all(before) is False
        assert all(after) is True
        assert all(r[2] for r in response) is True

        if as_paths:
            assert all(Path in type(r[0]).__bases__ for r in response) is True
        else:
            assert all(type(r[0]) == str for r in response) is True

    @pytest.mark.parametrize('prefix,folder,as_paths', [
        ("test_prefix", "test_folder", False),
        ("test_prefix", Path("test_folder"), False),
        (Path("test_prefix"), "test_folder", True),
        (Path("test_prefix"), Path("test_folder"), True),
    ])
    def test_download_prefix_to_folder(self, s3_client, prefix, folder, as_paths):

        paths = create_files(as_paths)

        lst = [(
            Path(p).as_posix().replace(self.root_folder, Path(prefix).as_posix()), p)
            for p in paths
        ]

        with create_bucket(s3_client, BUCKET_NAME, keys_paths=lst):
            response = download_prefix_to_folder(BUCKET_NAME, prefix, folder)

        result = dircmp(self.root_folder, folder)

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

        shutil.rmtree(self.root_folder)
        shutil.rmtree(folder)

        assert test_1_Root and test_2_Root and test_1_FolderA and test_1_FolderD
        assert len(response) == 4
