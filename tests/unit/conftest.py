from contextlib import contextmanager
from typing import (
    Dict,
    List
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
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"


@pytest.yield_fixture(scope="module")
def s3_client(aws_credentials):
    with mock_s3():
        session = boto3.session.Session()
        s3 = session.client("s3", region_name="us-east-1")
        yield s3


@contextmanager
def create_bucket(s3_client, bucket, key=None, data=None, keys_paths=[]):
    s3_client.create_bucket(Bucket=bucket)

    if key and data:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)

    for key, fn in keys_paths:
        s3_client.upload_file(Bucket=bucket, Key=key, Filename=fn)

    yield

    response = s3_client.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])

    s3_client.delete_bucket(Bucket=bucket)


def create_files(path: str, files: Dict) -> List[str]:
    """Create folder structure.

    The function creates a folder structure with files under a given path
    based on the dictionary passed as parameter.

    Parameters
    ----------
    path: str
        Root folder where the files/folders will be created.

    files: dict
        Dictionary with parametrization of files and folder to be created.
        A key is a folder if the value is an object, else
        A key is a file and the value will be the file content.

    Returns
    -------
    list[str]
        Path to all files/folders created.

    Examples
    --------
    To create the structure below

        root
        ├── file.root
        ├── folderA
        │   └── file.A2
        ├── folderB
        └── folderC
            └── folderC1
                └── file.CC1

    >>> files = {
            "file.root": "This is the content of the file.root",
            "folderA": {
                "file.A2": "This is the content of the file.A2"
            },
            "folderB": {
            },
            "folderC": {
                "folderC1": {
                    "file.CC1": "This is the content of the file.CC1"
                },
            }
        }

    >>> create_files('root_folder', files)
    [
        'root_folder/file.root',
        'root_folder/folderA/file.A2',
        'root_folder/folderC/folderC1/file.CC1'
    ]

    """
    filepaths: List[str] = []
    os.makedirs(path, exist_ok=True)
    for key in files:
        if type(files[key]) is dict:
            filepaths.extend(
                create_files(os.path.join(path, key), files[key])
            )
        else:
            with open(os.path.join(path, key), 'w') as f:
                f.write(files[key])
            filepaths.append(os.path.join(path, key))

    return filepaths
