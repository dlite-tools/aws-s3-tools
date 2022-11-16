"""Download S3 objects to files."""
from concurrent import futures
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import boto3

from s3_tools.objects.list import list_objects
from s3_tools.utils import (
    _create_progress_bar,
    _get_future_output,
)


def download_key_to_file(
    bucket: str,
    key: Union[str, Path],
    local_filename: Union[str, Path],
    progress=None,  # type: ignore # No import if extra not installed
    task_id: int = -1,
    aws_auth: Dict[str, str] = {},
    extra_args: Dict[str, str] = {},
) -> bool:
    """Retrieve one object from AWS S3 bucket and store into local disk.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key where the object is stored.

    local_filename: Union[str, Path]
        Local file where the data will be downloaded to.

    progress: rich.Progress
        Instance of a rich Progress bar, by default None.

    task_id: int
        Task ID on the progress bar to be updated, by default -1.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    extra_args: Dict[str, str]
        Extra arguments to be passed to the boto3 download_file method, by default is empty.
        Allowed download arguments:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS

    Returns
    -------
    bool
        True if the local file exists.

    Examples
    --------
    >>> download_key_to_file(
    ...     bucket="myBucket",
    ...     key="myData/myFile.data",
    ...     local_filename="theFile.data",
    ... )
    True

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")
    Path(local_filename).parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(
        Bucket=bucket,
        Key=Path(key).as_posix(),
        Filename=Path(local_filename).as_posix(),
        ExtraArgs=extra_args,
    )
    if progress:
        progress.update(task_id, advance=1)
    return Path(local_filename).exists()


def download_keys_to_files(
    bucket: str,
    keys_paths: List[Tuple[Union[str, Path], Union[str, Path]]],
    threads: int = 5,
    show_progress: bool = False,
    aws_auth: Dict[str, str] = {},
    as_paths: bool = False,
    default_extra_args: Dict[str, str] = {},
    extra_args_per_key: List[Dict[str, str]] = [],
) -> List[Tuple[Union[str, Path], Union[str, Path], Any]]:
    """Download list of objects to specific paths.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    keys_paths: List[Tuple[Union[str, Path], Union[str, Path]]]
        List with a tuple of S3 key to be downloaded and local path to be stored.
        e.g. [
            ("S3_Key", "Local_Path"),
            (Path("S3_Key"), "Local_Path"),
            ("S3_Key", Path("Local_Path")),
            (Path("S3_Key"), Path("Local_Path")),
        ]

    threads: int
        Number of parallel downloads, by default 5.

    show_progress: bool
        Show progress bar on console, by default False.
        (Need to install extra [progress] to be used)

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    as_paths: bool
        If True, the keys are returned as Path objects, otherwise as strings, by default is False.

    default_extra_args: Dict[str, str]
        Extra arguments to be passed to the boto3 download_file method, by default is empty.
        The extra arguments will be applied to all S3 keys.

    extra_args_per_key: List[Dict[str, str]]
        Extra arguments to be passed for each S3 key to the boto3 download_file method, by default is empty.
        The default extra arguments will be merged with the extra arguments passed for each key.

    Returns
    -------
    List[Tuple]
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
    ...         ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc"),
    ...     ]
    ... )
    [
        ("myData/myMusic/awesome.mp3", "MyFiles/myMusic/awesome.mp3", True),
        ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc", True),
        ("myData/myFile.data", "MyFiles/myFile.data", True),
    ]

    """
    if len(extra_args_per_key) != 0 and len(extra_args_per_key) != len(keys_paths):
        raise ValueError("The length of extra_args_per_key must be the same as keys_paths.")

    extra_arguments = [{}] * len(keys_paths) if len(extra_args_per_key) == 0 else extra_args_per_key

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
                aws_auth,
                {**default_extra_args, **extra_args},
            ): {"s3": s3_key, "fn": filename}
            for (s3_key, filename), extra_args in zip(keys_paths, extra_arguments)
        }

        output = [
            (executions[future]["s3"], executions[future]["fn"], _get_future_output(future))
            for future in futures.as_completed(executions)
        ]

    if show_progress:
        progress.stop()

    if as_paths:
        output = [(Path(key), Path(fn), result) for key, fn, result in output]
    else:
        output = [(Path(key).as_posix(), Path(fn).as_posix(), result) for key, fn, result in output]

    return output


def download_prefix_to_folder(
    bucket: str,
    prefix: Union[str, Path],
    folder: Union[str, Path],
    search_str: Optional[str] = None,
    remove_prefix: bool = True,
    threads: int = 5,
    show_progress: bool = False,
    aws_auth: Dict[str, str] = {},
    as_paths: bool = False,
    default_extra_args: Dict[str, str] = {},
) -> List[Tuple[Union[str, Path], Union[str, Path], Any]]:
    """Download objects to local folder.

    Function to retrieve all files under a prefix on S3 and store them into local folder.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the objects are stored.

    prefix: Union[str, Path]
        Prefix where the objects are under.

    folder: Union[str, Path]
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

    as_paths: bool
        If True, the keys are returned as Path objects, otherwise as strings, by default is False.

    default_extra_args: Dict[str, str]
        Extra arguments to be passed to the boto3 download_file method, by default is empty.
        The extra arguments will be applied to all S3 keys.

    Returns
    -------
    List[Tuple]
        A list with tuples formed by the "S3_Key", "Local_Path", and the result of the download.
        If successful will have True, if not will contain the error message.

    Examples
    --------
    >>> download_prefix_to_folder(
    ...     bucket="myBucket",
    ...     prefix="myData",
    ...     folder="myFiles",
    ... )
    [
        ("myData/myFile.data", "MyFiles/myFile.data", True),
        ("myData/myMusic/awesome.mp3", "MyFiles/myMusic/awesome.mp3", True),
        ("myData/myDocs/paper.doc", "MyFiles/myDocs/paper.doc", True),
    ]

    """
    s3_keys = list_objects(
        bucket=bucket,
        prefix=prefix,
        search_str=search_str,
        aws_auth=aws_auth,
        as_paths=as_paths,
    )

    keys_paths: List[Tuple[Union[str, Path], Union[str, Path]]] = [(
        key,
        "{}/{}".format(
            Path(folder).as_posix(),
            Path(key).as_posix().replace(Path(prefix).as_posix(), "")[1:] if remove_prefix else key
        )
    ) for key in s3_keys]

    return download_keys_to_files(bucket, keys_paths, threads, show_progress, aws_auth, as_paths, default_extra_args)
