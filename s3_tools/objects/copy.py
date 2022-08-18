"""Copy S3 objects."""
from concurrent import futures
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import boto3

from s3_tools.objects.list import list_objects


def copy_object(
    source_bucket: str,
    source_key: Union[str, Path],
    destination_bucket: str,
    destination_key: Union[str, Path],
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy S3 object from source bucket and key to destination.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the object is stored.

    source_key : Union[str, Path]
        S3 key where the object is referenced.

    destination_bucket : str
        S3 destination bucket.

    destination_key : Union[str, Path]
        S3 destination key.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Examples
    --------
    >>> copy_object(
    ...    source_bucket='bucket',
    ...    source_key='myFiles/song.mp3',
    ...    destination_bucket='bucket',
    ...    destination_key='myMusic/song.mp3',
    ... )

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.resource("s3")

    s3.meta.client.copy(
        {'Bucket': source_bucket, 'Key': Path(source_key).as_posix()},
        destination_bucket,
        Path(destination_key).as_posix()
    )


def copy_keys(
    source_bucket: str,
    source_keys: List[Union[str, Path]],
    destination_bucket: str,
    destination_keys: List[Union[str, Path]],
    threads: int = 5,
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy a list of S3 objects from source bucket to destination.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the objects are stored.

    source_keys : List[Union[str, Path]]
        S3 keys where the objects are referenced.

    destination_bucket : str
        S3 destination bucket.

    destination_keys : List[Union[str, Path]]
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
    ...         Path('myFiles/photo.jpg'),
    ...     ],
    ...     destination_bucket='bucket',
    ...     destination_keys=[
    ...         Path('myMusic/song.mp3'),
    ...         'myPhotos/photo.jpg',
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
    source_prefix: Union[str, Path],
    destination_bucket: str,
    change_prefix: Optional[Tuple[Union[str, Path], Union[str, Path]]] = None,
    filter_keys: Optional[str] = None,
    threads: int = 5,
    aws_auth: Dict[str, str] = {}
) -> None:
    """Copy S3 objects from source bucket to destination based on prefix filter.

    Parameters
    ----------
    source_bucket : str
        S3 bucket where the objects are stored.

    source_prefix : Union[str, Path]
        S3 prefix where the objects are referenced.

    destination_bucket : str
        S3 destination bucket.

    change_prefix : Tuple[Union[str, Path], Union[str, Path]], optional
        Text to be replaced in keys prefixes, by default is None.
        The first element is the text to be replaced, the second is the replacement text.

    filter_keys : str, optional
        Basic search string to filter out keys on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

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
    ...     filter_keys='*images*',
    ...     change_prefix=('myFiles', 'backup')
    ... )

    """
    source_keys = list_objects(
        bucket=source_bucket,
        prefix=source_prefix,
        search_str=filter_keys,
        aws_auth=aws_auth
    )

    destination_keys = source_keys if change_prefix is None else [
        Path(key).as_posix().replace(
            Path(change_prefix[0]).as_posix(),
            Path(change_prefix[1]).as_posix()
        )
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
