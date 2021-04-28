"""List S3 bucket objects."""
from typing import (
    Dict,
    Optional,
    List
)

import fnmatch

import boto3


def list_objects(
    bucket: str,
    prefix: str = "",
    search_str: Optional[str] = None,
    max_keys: int = 1000,
    aws_auth: Dict[str, str] = {}
) -> List[str]:
    """Retrieve the list of objects from AWS S3 bucket under a given prefix and search string.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    prefix: str
        Prefix where the objects are under.

    search_str: str
        Basic search string to filter out keys on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

    max_keys: int
        Max number of keys to have pagination.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    List[str]
        List of keys inside the bucket, under the path, and filtered.

    Examples
    --------
    >>> list_objects(bucket="myBucket", prefix="myData")
    [
        "myData/myFile.data",
        "myData/myMusic/awesome.mp3",
        "myData/myDocs/paper.doc"
    ]

    >>> list_objects(bucket="myBucket", prefix="myData", search_str="*paper*")
    [
        "myData/myDocs/paper.doc"
    ]

    """
    continuation_token: Optional[str] = None
    keys: List[str] = []

    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    while True:
        list_kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": max_keys
        }
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**list_kwargs)
        if "Contents" in response:
            keys.extend([obj["Key"] for obj in response["Contents"]])

        if not response.get("NextContinuationToken"):
            break

        continuation_token = response.get("NextContinuationToken")

    return keys if not search_str else fnmatch.filter(keys, search_str)
