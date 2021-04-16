"""Check objects on S3 bucket."""
import boto3


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
