"""Read S3 objects into variables."""
from pathlib import Path
from typing import (
    Any,
    Dict,
    Union,
)

import boto3
import ujson


def read_object_to_bytes(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> bytes:
    """Retrieve one object from AWS S3 bucket as a byte array.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key where the object is stored.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    bytes
        Object content as bytes.

    Examples
    --------
    >>> read_object_to_bytes(
    ...     bucket="myBucket",
    ...     key="myData/myFile.data",
    ... )
    b"The file content"

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=Path(key).as_posix())

    return obj["Body"].read()


def read_object_to_text(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> str:
    """Retrieve one object from AWS S3 bucket as a string.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key where the object is stored.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        Object content as string.

    Examples
    --------
    >>> read_object_to_text(
    ...     bucket="myBucket",
    ...     key="myData/myFile.data"
    ... )
    "The file content"

    """
    data = read_object_to_bytes(bucket, key, aws_auth)
    return data.decode("utf-8")


def read_object_to_dict(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> Dict[Any, Any]:
    """Retrieve one object from AWS S3 bucket as a dictionary.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key where the object is stored.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    Dict[Any, Any]
        Object content as dictionary.

    Examples
    --------
    >>> read_object_to_dict(
    ...     bucket="myBucket",
    ...     key="myData/myFile.json",
    ... )
    {"key": "value", "1": "text"}

    """
    data = read_object_to_bytes(bucket, key, aws_auth)
    return ujson.loads(data.decode("utf-8"))
