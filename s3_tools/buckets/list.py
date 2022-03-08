"""List S3 Buckets."""
import fnmatch
from typing import (
    Dict,
    Optional,
    List
)


import boto3


def list_buckets(search_str: Optional[str] = None, aws_auth: Dict[str, str] = {}) -> List[str]:
    """Retrieve the list of buckets from AWS S3 filtered by search string.

    Parameters
    ----------
    search_str: str
        Basic search string to filter out buckets on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    List[str]
        List of bucket names filtered.

    Examples
    --------
    >>> list_buckets()
    [ "myRawData", "myProcessedData", "myFinalData"]

    >>> list_buckets(search_str="*Raw*")
    [ "myRawData" ]

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    response = s3.list_buckets()

    buckets = [bucket["Name"] for bucket in response["Buckets"]]

    return buckets if not search_str else fnmatch.filter(buckets, search_str)
