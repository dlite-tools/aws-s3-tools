"""General utilities."""
from typing import Any
from concurrent import futures

import boto3


def _get_future_output(future: futures.Future) -> Any:
    """Get a futures.Future result or exception message.

    Parameters
    ----------
    future : futures.Future
        An async callable method.

    Returns
    -------
    Any
        If execution has no error will return the value returned by the callable method,
        else will return the exception message.
    """
    try:
        return future.result()
    except Exception as e:
        return repr(e)


def object_exists(bucket: str, key: str) -> bool:
    """Check if an object exists for a given bucket and key.

    Parameters
    ----------
    bucket : str
        Bucket name where the object is stored.
    key : str
        Full key for the object.

    Returns
    -------
    bool
        True if the object exists, otherwise False.

    Example
    -------
    >>> object_exists("myBucket", "myFiles/music.mp3")
    True
    """
    session = boto3.session.Session()
    s3 = session.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        return False

    return True
