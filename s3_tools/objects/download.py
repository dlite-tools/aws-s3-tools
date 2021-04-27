"""Download S3 objects to files."""
from concurrent import futures
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple
)

import boto3

from s3_tools.objects.list import list_objects
from s3_tools.utils import (
    _create_progress_bar,
    _get_future_output
)


def download_key_to_file(
    bucket: str,
    key: str,
    local_filename: str,
    progress=None,  # type: ignore # No import if extra not installed
    task_id: int = -1,
    aws_auth: Dict[str, str] = {}
) -> bool:
    """Retrieve one object from AWS S3 bucket and store into local disk.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: str
        Key where the object is stored.

    local_filename: str
        Local file where the data will be downloaded to.

    progress: rich.Progress
        Instance of a rich Progress bar, by default None.

    task_id: int
        Task ID on the progress bar to be updated, by default -1.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    bool
        True if the local file exists.

    Examples
    --------
    >>> read_object_to_file(
    ...     bucket="myBucket",
    ...     key="myData/myFile.data",
    ...     local_filename="theFile.data"
    ... )
    True

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")
    Path(local_filename).parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(Bucket=bucket, Key=key, Filename=local_filename)
    if progress:
        progress.update(task_id, advance=1)
    return Path(local_filename).exists()


def download_keys_to_files(
    bucket: str,
    keys_paths: List[Tuple[str, str]],
    threads: int = 5,
    show_progress: bool = False,
    aws_auth: Dict[str, str] = {}
) -> List[Tuple[str, str, Any]]:
    """Download list of objects to specific paths.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    keys_paths: List[Tuple[str, str]]
        List with a tuple of S3 key to be downloaded and local path to be stored.
        e.g. [("S3_Key", "Local_Path"), ("S3_Key", "Local_Path")]

    threads: int
        Number of parallel downloads, by default 5.

    show_progress: bool
        Show progress bar on console, by default False.
        (Need to install extra [progress] to be used)

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    list of tuples
        A list with tuples formed by the "S3_Key", "Local_Path", and the result of the download.
        If successful will have True, if not will contain the error message.
        Attention, the output list may not follow the same input order.

    Examples
    --------
    >>> download_keys_to_files(
    ...     bucket="myBucket",
    ...     keys_paths=[
    ...         ("myData/myFile.data", "MyFiles/myFile.data"),
    ...         ("myData/myMusic/awesome.mp3", "MyFiles/myMusic/awesome.mp3"),
    ...         ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc")
    ...     ]
    ... )
    [
        ("myData/myMusic/awesome.mp3", "MyFiles/myMusic/awesome.mp3", True),
        ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc", True),
        ("myData/myFile.data", "MyFiles/myFile.data", True)
    ]

    """
    if show_progress:
        progress, task_id = _create_progress_bar("Downloading", len(keys_paths))
        progress.start()
        progress.start_task(task_id)
    else:
        progress, task_id = None, -1

    with futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # Create a dictionary to map the future execution with the (S3 key, Local filename)
        # dict = {future: values}
        executions = {
            executor.submit(
                download_key_to_file,
                bucket,
                s3_key,
                filename,
                progress,
                task_id,
                aws_auth
            ): {"s3": s3_key, "fn": filename}
            for s3_key, filename in keys_paths
        }

        output = [
            (executions[future]["s3"], executions[future]["fn"], _get_future_output(future))
            for future in futures.as_completed(executions)
        ]

    if show_progress:
        progress.stop()

    return output


def download_prefix_to_folder(
    bucket: str,
    prefix: str,
    folder: str,
    search_str: Optional[str] = None,
    remove_prefix: bool = True,
    threads: int = 5,
    show_progress: bool = False,
    aws_auth: Dict[str, str] = {}
) -> List[Tuple[str, str, Any]]:
    """Download objects to local folder.

    Function to retrieve all files under a prefix on S3 and store them into local folder.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    prefix: str
        Prefix where the objects are under.

    folder: str
        Local folder path where files will be stored.

    search_str: str
        Basic search string to filter out keys on result (uses Unix shell-style wildcards), by default is None.
        For more about the search check "fnmatch" package.

    remove_prefix: bool
        If True will remove the the prefix when writing to local folder.
        The remaining "folders" on the key will be created on the local folder.

    threads: int
        Number of parallel downloads, by default 5.

    show_progress: bool
        Show progress bar on console, by default False.
        (Need to install extra [progress] to be used)

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    List[Tuples]
        A list with tuples formed by the "S3_Key", "Local_Path", and the result of the download.
        If successful will have True, if not will contain the error message.

    Examples
    --------
    >>> download_prefix_to_folder(
    ...     bucket="myBucket",
    ...     prefix="myData",
    ...     folder="myFiles"
    ... )
    [
        ("myData/myFile.data", "MyFiles/myFile.data", True),
        ("myData/myMusic/awesome.mp3", "MyFiles/myMusic/awesome.mp3", True),
        ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc", True)
    ]

    """
    s3_keys = list_objects(bucket=bucket, prefix=prefix, search_str=search_str, aws_auth=aws_auth)

    keys_paths = [(
        key,
        "{}/{}".format(folder, key.replace(prefix, "")[1:] if remove_prefix else key)
    ) for key in s3_keys]

    return download_keys_to_files(bucket, keys_paths, threads, show_progress, aws_auth)
