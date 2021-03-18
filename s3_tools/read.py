"""Read S3 objects into variables."""
from typing import Dict

import boto3
import ujson


def read_object_to_bytes(bucket: str, key: str) -> bytes:
    """Retrieve one object from AWS S3 bucket as a byte array.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: str
        Key where the object is stored.

    Returns
    -------
    bytes
        Object content as bytes.

    Raises
    ------
    KeyError
        When the key 'Body' is missing on the response.

    Examples
    --------
    >>> read_object_to_bytes(
    ...     bucket="myBucket",
    ...     key="myData/myFile.data"
    ... )
    b"The file content"

    """
    session = boto3.session.Session()
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)

    if "Body" not in obj:
        raise KeyError("Missing key 'Body' on response")

    return obj["Body"].read()


def read_object_to_text(bucket: str, key: str) -> str:
    """Retrieve one object from AWS S3 bucket as a string.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: str
        Key where the object is stored.

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
    data = read_object_to_bytes(bucket, key)
    return data.decode("utf-8")


def read_object_to_dict(bucket: str, key: str) -> Dict:
    """Retrieve one object from AWS S3 bucket as a dictionary.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: str
        Key where the object is stored.

    Returns
    -------
    dict
        Object content as dictionary.

    Examples
    --------
    >>> read_object_to_dict(
    ...     bucket="myBucket",
    ...     key="myData/myFile.json"
    ... )
    {"key": "value", "1": "text"}

    """
    data = read_object_to_bytes(bucket, key)
    return ujson.loads(data.decode("utf-8"))
