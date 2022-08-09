"""Delete objects from S3 bucket."""
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Union,
)

import boto3

from s3_tools.objects.list import list_objects


def delete_object(bucket: str, key: Union[str, Path], aws_auth: Dict[str, str] = {}) -> None:
    """Delete a given object from S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key for the object that will be deleted.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Examples
    --------
    >>> delete_object(bucket="myBucket", key="myData/myFile.data")

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")
    s3.delete_object(Bucket=bucket, Key=Path(key).as_posix())


def delete_prefix(
    bucket: str,
    prefix: Union[str, Path],
    dry_run: bool = True,
    aws_auth: Dict[str, str] = {}
) -> Optional[List[Union[str, Path]]]:
    """Delete all objects under the given prefix from S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    prefix: Union[str, Path]
        Prefix where the objects are under.

    dry_run: bool
         If True will not delete the objects.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    List[Union[str, Path]]
        List of S3 keys to be deleted if dry_run True, else None.

    Examples
    --------
    >>> delete_prefix(bucket="myBucket", prefix="myData")
    [
        "myData/myMusic/awesome.mp3",
        "myData/myDocs/paper.doc"
    ]

    >>> delete_prefix(bucket="myBucket", prefix=Path("myData"), dry_run=False)

    """
    keys = list_objects(bucket, prefix, aws_auth=aws_auth)

    if dry_run:
        return [key for key in keys]

    for key in keys:
        delete_object(bucket, key, aws_auth)

    return None


def delete_keys(bucket: str, keys: List[Union[str, Path]], dry_run: bool = True, aws_auth: Dict[str, str] = {}) -> None:
    """Delete all objects in the keys list from S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    keys: List[Union[str, Path]]
        List of object keys.

    dry_run: bool
         If True will not delete the objects.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Examples
    --------
    >>> delete_keys(
    ...     bucket="myBucket",
    ...     keys=[
    ...         "myData/myMusic/awesome.mp3",
    ...         Path("myData/myDocs/paper.doc")
    ...     ],
    ...     dry_run=False
    ... )

    """
    if dry_run:
        return

    for key in keys:
        delete_object(bucket, key, aws_auth)
