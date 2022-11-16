"""Check objects on S3 bucket."""
from pathlib import Path
from typing import (
    Any,
    Dict,
    Union,
)

import boto3
from botocore.exceptions import ClientError


def object_exists(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> bool:
    """Check if an object exists for a given bucket and key.

    Parameters
    ----------
    bucket : str
        Bucket name where the object is stored.

    key : Union[str, Path]
        Full key for the object.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    bool
        True if the object exists, otherwise False.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Example
    -------
    >>> object_exists("myBucket", "myFiles/music.mp3")
    True
    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=Path(key).as_posix())
    except Exception as error:
        if isinstance(error, ClientError) and (error.response["Error"]["Code"] == "404"):
            return False

        raise error  # Raise anything different from Not Found

    return True


def object_metadata(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> Dict[str, Any]:
    """Get metadata from an S3 object.

    Parameters
    ----------
    bucket : str
        Bucket name where the object is stored.

    key : Union[str, Path]
        Full key for the object.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    Dict[str, Any]
        Metadata from the object.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Example
    -------
    >>> object_metadata("myBucket", "myFiles/music.mp3")
    {
        'ResponseMetadata': {},
        'AcceptRanges': 'bytes',
        'LastModified': datetime.datetime(2020, 10, 31, 20, 46, 13, tzinfo=tzutc()),
        'ContentLength': 123456,
        'ETag': '"1234567890abcdef1234567890abcdef"',
        'ContentType': 'audio/mpeg',
        'Metadata': {}
    }
    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        return s3.head_object(Bucket=bucket, Key=Path(key).as_posix())
    except Exception as error:
        if isinstance(error, ClientError) and (error.response["Error"]["Code"] == "404"):
            return {}

        raise error  # Raise anything different from Not Found
