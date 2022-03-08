"""Check S3 bucket."""
from typing import Dict

import boto3
from botocore.exceptions import ClientError


def bucket_exists(bucket: str, aws_auth: Dict[str, str] = {}) -> bool:
    """Check if a bucket exists.

    Parameters
    ----------
    bucket : str
        Bucket name to be checked.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    bool
        True if the bucket exists, otherwise False.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Example
    -------
    >>> bucket_exists("myBucket")
    True
    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as error:
        if isinstance(error, ClientError) and (error.response["Error"]["Code"] == "404"):
            return False

        raise error  # Raise anything different from Not Found

    return True
