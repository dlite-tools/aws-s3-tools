"""Create S3 Bucket."""
from typing import Dict

import boto3


def create_bucket(name: str, configs: Dict[str, str] = {}, aws_auth: Dict[str, str] = {}) -> bool:
    """Create an S3 bucket.

    Parameters
    ----------
    name : str
        Name of the bucket to create.

    configs : Dict[str, str]
        Bucket configurations, by default is empty.
        To know more about it check boto3 documentation.

    aws_auth : Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    bool
        True if the bucket was created, False otherwise.

    Examples
    --------
    >>> create_bucket("myBucket")
    True

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    response = s3.create_bucket(Bucket=name, **configs)

    return response['ResponseMetadata']['HTTPStatusCode'] == 200
