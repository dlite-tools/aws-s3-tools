"""Copy S3 objects."""
from concurrent import futures
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

import boto3

from s3_tools.objects.list import list_objects


def copy_object(
    source_bucket: str,
    source_key: str,
    destination_bucket: str,
    destination_key: str,
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy S3 object from source bucket and key to destination.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the object is stored.

    source_key : str
        S3 key where the object is referenced.

    destination_bucket : str
        S3 destination bucket.

    destination_key : str
        S3 destination key.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Examples
    --------
    >>> copy_object(
    ...    source_bucket='bucket',
    ...    source_key='myFiles/song.mp3',
    ...    destination_bucket='bucket',
    ...    destination_key='myMusic/song.mp3'
    ... )

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.resource("s3")

    s3.meta.client.copy(
        {'Bucket': source_bucket, 'Key': source_key},
        destination_bucket,
        destination_key
    )


def copy_keys(
    source_bucket: str,
    source_keys: List[str],
    destination_bucket: str,
    destination_keys: List[str],
    threads: int = 5,
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy a list of S3 objects from source bucket to destination.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the objects are stored.

    source_keys : List[str]
        S3 keys where the objects are referenced.

    destination_bucket : str
        S3 destination bucket.

    destination_keys : List[str]
        S3 destination keys.

    threads : int, optional
        Number of parallel uploads, by default 5.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Raises
    ------
    IndexError
        When the source_keys and destination_keys have different length.

    ValueError
        When the keys list is empty.

    Examples
    --------
    >>> copy_keys(
    ...     source_bucket='bucket',
    ...     source_keys=[
    ...         'myFiles/song.mp3',
    ...         'myFiles/photo.jpg'
    ...     ],
    ...     destination_bucket='bucket',
    ...     destination_keys=[
    ...         'myMusic/song.mp3',
    ...         'myPhotos/photo.jpg'
    ...     ]
    ... )

    """
    if len(source_keys) != len(destination_keys):
        raise IndexError("Key lists must have the same length")

    if len(source_keys) == 0:
        raise ValueError("Key list length must be greater than zero")

    with futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executors = (
            executor.submit(copy_object, source_bucket, source, destination_bucket, destination, aws_auth)
            for source, destination in zip(source_keys, destination_keys)
        )

        for ex in executors:
            ex.result()


def copy_prefix(
    source_bucket: str,
    source_prefix: str,
    destination_bucket: str,
    source_search_match: Optional[str] = None,
    replace_in_prefix: Optional[Tuple[str, str]] = None,
    threads: int = 5,
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy S3 objects from source bucket to destination based on prefix filter.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the objects are stored.

    source_prefix : str
        S3 prefix where the objects are referenced.

    destination_bucket : str
        S3 destination bucket.

    source_search_match : str, optional
        Basic search string to filter out keys on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

    replace_in_prefix : Tuple[str, str], optional
        Text to be replaced in keys prefixes, by default is None.
        The first element is the text to be replaced, the second is the replacement text.

    threads : int, optional
        Number of parallel uploads, by default 5.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Examples
    --------
    >>> copy_prefix(
    ...     source_bucket='MyBucket',
    ...     source_prefix='myFiles',
    ...     destination_bucket='OtherBucket',
    ...     source_search_match='*images*',
    ...     replace_in_prefix=('myFiles', 'backup')
    ... )

    """
    source_keys = list_objects(
        bucket=source_bucket,
        prefix=source_prefix,
        search_str=source_search_match,
        aws_auth=aws_auth
    )

    destination_keys = source_keys if replace_in_prefix is None else [
        key.replace(replace_in_prefix[0], replace_in_prefix[1])
        for key in source_keys
    ]

    copy_keys(
        source_bucket=source_bucket,
        source_keys=source_keys,
        destination_bucket=destination_bucket,
        destination_keys=destination_keys,
        threads=threads,
        aws_auth=aws_auth
    )
