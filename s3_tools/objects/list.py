"""List S3 bucket objects."""
from pathlib import Path
from typing import (
    Dict,
    Optional,
    List,
    Union,
)

import fnmatch

import boto3


def list_objects(
    bucket: str,
    prefix: Union[str, Path] = "",
    search_str: Optional[str] = None,
    max_keys: int = 1000,
    aws_auth: Dict[str, str] = {},
    as_paths: bool = False,
) -> List[Union[str, Path]]:
    """Retrieve the list of objects from AWS S3 bucket under a given prefix and search string.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    prefix: Union[str, Path]
        Prefix where the objects are under.

    search_str: str
        Basic search string to filter out keys on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

    max_keys: int
        Max number of keys to have pagination.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    as_paths: bool
        If True, the keys are returned as Path objects, otherwise as strings, by default is False.

    Returns
    -------
    List[Union[str, Path]]
        List of keys inside the bucket, under the path, and filtered.

    Examples
    --------
    >>> list_objects(bucket="myBucket", prefix="myData")
    [
        "myData/myFile.data",
        "myData/myMusic/awesome.mp3",
        "myData/myDocs/paper.doc"
    ]

    >>> list_objects(bucket="myBucket", prefix="myData", search_str="*paper*", as_paths=True)
    [
        Path("myData/myDocs/paper.doc")
    ]

    """
    continuation_token: Optional[str] = None
    keys = []

    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    while True:
        list_kwargs = {
            "Bucket": bucket,
            "Prefix": Path(prefix).as_posix(),
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

    if isinstance(search_str, str):
        keys = fnmatch.filter(keys, search_str)

    return keys if not as_paths else [Path(key) for key in keys]
