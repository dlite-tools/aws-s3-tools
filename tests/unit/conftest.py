from contextlib import contextmanager
from pathlib import Path
from typing import (
    List,
    Union,
)
import os

import boto3
import pytest

from moto import mock_s3

BUCKET_NAME = "mock"
FILENAME = "tests/resources/mock_file.csv"
EMPTY_FILE = "tests/resources/empty.data"


@pytest.fixture(scope="module")
def aws_credentials():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"


@pytest.yield_fixture(scope="module")
def s3_client(aws_credentials):
    with mock_s3():
        session = boto3.session.Session()
        s3 = session.client("s3")
        yield s3


@contextmanager
def create_buckets(s3_client, names):
    for name in names:
        s3_client.create_bucket(Bucket=name)
    yield
    for name in names:
        s3_client.delete_bucket(Bucket=name)


@contextmanager
def create_bucket(s3_client, bucket, key=None, data=None, keys_paths=[]):
    s3_client.create_bucket(Bucket=bucket)

    if key and data:
        s3_client.put_object(Bucket=bucket, Key=Path(key).as_posix(), Body=data)

    for key, fn in keys_paths:
        s3_client.upload_file(
            Bucket=bucket,
            Key=Path(key).as_posix(),
            Filename=Path(fn).as_posix()
        )

    yield

    response = s3_client.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])

    s3_client.delete_bucket(Bucket=bucket)


def create_files(as_paths: bool = False) -> List[Union[str, Path]]:
    """Create folder structure.

    The function creates a folder structure with files under a path.

    Parameters
    ----------
    as_paths: bool
        If True, the keys are returned as Path objects, otherwise as strings, by default is False.


    Returns
    -------
    List[Union[str, Path]]
        Path to all files/folders created.

    Examples
    --------
    >>> create_files()
    [
        'root_folder/file.root',
        'root_folder/folderA/file.A2',
        'root_folder/folderC/folderC1/file.CC1'
    ]

    """
    files = {
        "TEST_ROOT_A/file.root": "This file is in the root folder",
        "TEST_ROOT_A/folderA/file.A1": "This file is in the folder A - file A1",
        "TEST_ROOT_A/folderA/file.A2": "This file is in the folder A - file A2",
        "TEST_ROOT_A/folderB": "",
        "TEST_ROOT_A/folderC/folderD/file.D1": "This file is in the folder D - file D1",
    }

    for key, content in files.items():
        fn = Path(key)
        if len(content) > 0:
            fn.parent.mkdir(parents=True, exist_ok=True)
            fn.open('w').write(content)
        else:
            fn.mkdir(parents=True, exist_ok=True)

    return [
        Path(key) if as_paths else key
        for key, content in files.items()
        if len(content) > 0
    ]
