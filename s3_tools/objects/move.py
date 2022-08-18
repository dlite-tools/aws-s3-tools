"""Move S3 objects."""
from pathlib import Path
from concurrent import futures
from typing import (
    Dict,
    List,
    Union,
)

import boto3

from s3_tools.objects.delete import delete_object


def move_object(
    source_bucket: str,
    source_key: Union[str, Path],
    destination_bucket: str,
    destination_key: Union[str, Path],
    aws_auth: Dict[str, str] = {},
) -> None:
    """Move S3 object from source bucket and key to destination.

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
    >>> move_object(
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
        Path(destination_key).as_posix(),
    )

    delete_object(source_bucket, source_key, aws_auth)


def move_keys(
    source_bucket: str,
    source_keys: List[Union[str, Path]],
    destination_bucket: str,
    destination_keys: List[Union[str, Path]],
    threads: int = 5,
    aws_auth: Dict[str, str] = {},
) -> None:
    """Move a list of S3 objects from source bucket to destination.

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
    >>> move_keys(
    ...     source_bucket='bucket',
    ...     source_keys=[
    ...         'myFiles/song.mp3',
    ...         'myFiles/photo.jpg',
    ...     ],
    ...     destination_bucket='bucket',
    ...     destination_keys=[
    ...         'myMusic/song.mp3',
    ...         'myPhotos/photo.jpg',
    ...     ],
    ... )

    """
    if len(source_keys) != len(destination_keys):
        raise IndexError("Key lists must have the same length")

    if len(source_keys) == 0:
        raise ValueError("Key list length must be greater than zero")

    with futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executors = (
            executor.submit(move_object, source_bucket, source, destination_bucket, destination, aws_auth)
            for source, destination in zip(source_keys, destination_keys)
        )

        for ex in executors:
            ex.result()
