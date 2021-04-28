"""Check objects on S3 bucket."""
from typing import Dict

import boto3
from botocore.exceptions import ClientError


def object_exists(bucket: str, key: str, aws_auth: Dict[str, str] = {}) -> bool:
    """Check if an object exists for a given bucket and key.

    Parameters
    ----------
    bucket : str
        Bucket name where the object is stored.

    key : str
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
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        if isinstance(error, ClientError) and (error.response["Error"]["Code"] == "404"):
            return False

        raise error  # Raise anything different from Not Found

    return True
