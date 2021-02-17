"""Upload files to S3 bucket."""
from concurrent import futures
from pathlib import Path
from typing import (
    Any,
    List,
    Tuple
)

import boto3

from .utils import _get_future_output


def upload_file_to_key(bucket: str, key: str, local_filename: str) -> str:
    """Upload one file from local disk and store into AWS S3 bucket.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object will be stored.

    key: str
        Key where the object will be stored.

    local_filename: str
        Local file from where the data will be uploaded.

    Returns
    -------
    str
        The S3 full URL to the file.

    Examples
    --------
    >>> write_object_from_file(
    ...     bucket="myBucket",
    ...     key="myFiles/music.mp3",
    ...     local_filename="files/music.mp3"
    ... )
    http://s3.amazonaws.com/myBucket/myFiles/music.mp3

    """
    session = boto3.session.Session()
    s3 = session.client("s3")
    s3.upload_file(Bucket=bucket, Key=key, Filename=local_filename)
    return "{}/{}/{}".format(s3.meta.endpoint_url, bucket, key)


def upload_files_to_keys(
    bucket: str,
    paths_keys: List[Tuple[str, str]],
    threads: int = 5
) -> List[Tuple[str, str, Any]]:
    """Upload list of files to specific objects.

    Parameters
    ----------
    bucket : str
        AWS S3 bucket where the objects will be stored.

    paths_keys : List[Tuple[str, str]]
        List with a tuple of local path to be uploaded and S3 key destination.
        e.g. [("Local_Path", "S3_Key"), ("Local_Path", "S3_Key")]

    threads : int, optional
        Number of parallel uploads, by default 5.

    Returns
    -------
    List[Tuple[str, str, Any]]
        A list with tuples formed by the "Local_Path", "S3_Key", and the result of the upload.
        If successful will have True, if not will contain the error message.
        Attention, the output list may not follow the same input order.

    Examples
    --------
    >>> upload_files_to_keys(
    ...     bucket="myBucket",
    ...     paths_keys=[
    ...         ("MyFiles/myFile.data", "myData/myFile.data"),
    ...         ("MyFiles/myMusic/awesome.mp3", "myData/myMusic/awesome.mp3"),
    ...         ("MyFiles/myDocs/paper.doc", "myData/myDocs/paper.doc")
    ...     ]
    ... )
    [
        ("MyFiles/myMusic/awesome.mp3", "myData/myMusic/awesome.mp3", True),
        ("MyFiles/myDocs/paper.doc", "myData/myDocs/paper.doc", True),
        ("MyFiles/myFile.data", "myData/myFile.data", True)
    ]

    """
    with futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # Create a dictionary to map the future execution with the (S3 key, Local filename)
        # dict = {future: values}
        executions = {
            executor.submit(upload_file_to_key, bucket, s3_key, filename): {"s3": s3_key, "fn": filename}
            for filename, s3_key in paths_keys
        }

        return [
            (executions[future]["fn"], executions[future]["s3"], _get_future_output(future))
            for future in futures.as_completed(executions)
        ]


def upload_folder_to_prefix(
    bucket: str,
    prefix: str,
    folder: str,
    search_str: str = "*",
    threads: int = 5
) -> List[Tuple[str, str, Any]]:
    """Upload local folder to a S3 prefix.

    Function to upload all files for a given folder (recursive)
    and store them into a S3 bucket under a prefix.
    The local folder structure will be replicated into S3.

    Parameters
    ----------
    bucket : str
        AWS S3 bucket where the object will be stored.

    prefix : str
        Prefix where the objects will be under.

    folder : str
        Local folder path where files are stored.
        Prefer to use the full path for the folder.

    search_str : str.
        A match string to select all the files to upload, by default "*".
        The string follows the rglob function pattern from the pathlib package.

    threads : int, optional
        Number of parallel uploads, by default 5

    Returns
    -------
    List[Tuple[str, str, Any]]
        A list with tuples formed by the "Local_Path", "S3_Key", and the result of the upload.
        If successful will have True, if not will contain the error message.

    Examples
    --------
    >>> upload_folder_to_prefix(
    ...     bucket="myBucket",
    ...     prefix="myFiles",
    ...     folder="/usr/files",
    ... )
    [
        ("/usr/files/music.mp3", "myFiles/music.mp3", True),
        ("/usr/files/awesome.wav", "myFiles/awesome.wav", True),
        ("/usr/files/data/metadata.json", "myFiles/data/metadata.json", True)
    ]

    """
    paths = [p for p in Path(folder).rglob(search_str) if p.is_file()]

    paths_keys = [
        (
            p.as_posix(),
            Path(prefix).joinpath(p.relative_to(Path(folder))).as_posix()  # S3 key
        )
        for p in paths
    ]

    return upload_files_to_keys(bucket, paths_keys, threads)
