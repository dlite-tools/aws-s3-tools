"""Write variables into S3 objects."""
import json
from typing import Dict

import boto3


def write_object_from_bytes(bucket: str, key: str, data: bytes, aws_auth: Dict[str, str] = {}) -> str:
    """Upload a bytes object to an object into AWS S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object will be stored.

    key: str
        Key where the object will be stored.

    data: bytes
        The object data to be uploaded to AWS S3.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        The S3 full URL to the file.

    Raises
    ------
    TypeError
        If data is not a bytes type.

    Examples
    --------
    >>> data = bytes("String to bytes", "utf-8")
    >>> write_object_from_bytes(
    ...     bucket="myBucket",
    ...     key="myFiles/file.data",
    ...     data=data
    ... )
    http://s3.amazonaws.com/myBucket/myFiles/file.data

    """
    if not isinstance(data, bytes):
        raise TypeError("Object data must be bytes type")

    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return "{}/{}/{}".format(s3.meta.endpoint_url, bucket, key)


def write_object_from_text(bucket: str, key: str, data: str, aws_auth: Dict[str, str] = {}) -> str:
    """Upload a string to an object into AWS S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object will be stored.

    key: str
        Key where the object will be stored.

    data: str
        The object data to be uploaded to AWS S3.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        The S3 full URL to the file.

    Raises
    ------
    TypeError
        If data is not a str type.

    Examples
    --------
    >>> data = "A very very not so long text"
    >>> write_object_from_text(
    ...     bucket="myBucket",
    ...     key="myFiles/file.txt",
    ...     data=data
    ... )
    http://s3.amazonaws.com/myBucket/myFiles/file.txt

    """
    if not isinstance(data, str):
        raise TypeError("Object data must be string type")

    return write_object_from_bytes(bucket, key, data.encode(), aws_auth)


def write_object_from_dict(bucket: str, key: str, data: Dict, aws_auth: Dict[str, str] = {}) -> str:
    """Upload a dictionary to an object into AWS S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object will be stored.

    key: str
        Key where the object will be stored.

    data: dict
        The object data to be uploaded to AWS S3.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        The S3 full URL to the file.

    Raises
    ------
    TypeError
        If `data` is not a dict type.

    Examples
    --------
    >>> data = {"key": "value", "1": "text"}
    >>> write_object_from_dict(
    ...     bucket="myBucket",
    ...     key="myFiles/file.json",
    ...     data=data
    ... )
    http://s3.amazonaws.com/myBucket/myFiles/file.json

    """
    if not isinstance(data, dict):
        raise TypeError("Object data must be dictionary type")

    return write_object_from_bytes(bucket, key, json.dumps(data).encode(), aws_auth)
