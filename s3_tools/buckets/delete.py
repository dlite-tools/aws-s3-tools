"""Delete S3 bucket."""
from typing import Dict

import boto3
from botocore.exceptions import ClientError


def delete_bucket(name: str, aws_auth: Dict[str, str] = {}) -> bool:
    """Delete an S3 bucket.

    Parameters
    ----------
    name : str
        Name of the bucket to delete.

    aws_auth : Dict[str, str], optional
        Contains AWS credentials, by default {}

    Returns
    -------
    bool
        True if the bucket was deleted, False otherwise.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Examples
    --------
    >>> delete_bucket("myBucket")
    True

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        response = s3.delete_bucket(Bucket=name)
    except Exception as error:
        if isinstance(error, ClientError) and (error.response["Error"]["Code"] == "NoSuchBucket"):
            return False
        else:
            raise error

    return response['ResponseMetadata']['HTTPStatusCode'] == 204
