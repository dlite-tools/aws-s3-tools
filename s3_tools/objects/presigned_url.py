"""Create presigned URL for S3 bucket objects."""
from pathlib import Path
from typing import (
    Dict,
    Optional,
    Union,
)

import boto3


def get_presigned_url(
    client_method: str,
    method_parameters: Optional[dict] = None,
    http_method: Optional[str] = None,
    expiration: int = 300,
    aws_auth: Dict[str, str] = {},
) -> str:
    """Generate a presigned URL to invoke an S3.Client method.

    Parameters
    ----------
    client_method: str
        Name of the S3.Client method, e.g., 'list_buckets'.

    method_parameters: Optional[dict]
        Dictionary of parameters to send to the method.

    expiration: int
        Time in seconds for the presigned URL to remain valid, default 5 minutes.

    http_method: Optional[str]
        HTTP method to use, e.g., GET, POST. If not specified, will automatically be select the appropriate method.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        Presigned URL.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Examples
    --------
    >>> get_presigned_url(
    ...    client_method='list_objects',
    ...    method_parameters={'Bucket': 'myBucket'},
    ... )
    https://myBucket.s3.amazonaws.com/?encoding-type=url&AWSAccessKeyId=ASI&Signature=5JLAcSKQ%3D&x-amz-security-token=FwoGZXIvY%&Expires=1646759818

    """
    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        response = s3.generate_presigned_url(
            ClientMethod=client_method,
            Params=method_parameters,
            ExpiresIn=expiration,
            HttpMethod=http_method,
        )
    except Exception as error:
        raise error

    return response


def get_presigned_download_url(
    bucket: str,
    key: Union[str, Path],
    expiration: int = 300,
    aws_auth: Dict[str, str] = {},
) -> str:
    """Generate a presigned URL to download an S3 object.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object is stored.

    key: Union[str, Path]
        Key for the object that will be downloaded.

    expiration: int
        Time in seconds for the presigned URL to remain valid, default 5 minutes.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    str
        Presigned URL.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Examples
    --------
    >>> import requests     # To install: pip install requests
    >>> url = get_presigned_download_url(
    ...    bucket='myBucket',
    ...    key='myData/myFile.data',
    ... )
    >>> response = requests.get(url)

    """
    return get_presigned_url(
        client_method='get_object',
        method_parameters={'Bucket': bucket, 'Key': Path(key).as_posix()},
        expiration=expiration,
        aws_auth=aws_auth,
    )


def get_presigned_upload_url(
    bucket: str,
    key: Union[str, Path],
    fields: Optional[dict] = None,
    conditions: Optional[list] = None,
    expiration: int = 300,
    aws_auth: Dict[str, str] = {},
) -> dict:
    """Generate a presigned URL S3 POST request to upload a file.

    Parameters
    ----------
    bucket: str
        AWS S3 bucket where the object will be stored.

    key: Union[str, Path]
        Key for the object that will will be stored.

    fields: Optional[dict]
        Dictionary of prefilled form fields.

    conditions: Optional[list]
        List of conditions to include in the policy.

    expiration: int
        Time in seconds for the presigned URL to remain valid, default 5 minutes.

    aws_auth: Dict[str, str]
        Contains AWS credentials, by default is empty.

    Returns
    -------
    dict
        A dictionary with two elements: url and fields.
        Url is the url to post to.
        Fields is a dictionary filled with the form fields and respective values to use when submitting the post.

    Raises
    ------
    Exception
        Any problem with the request is raised.

    Examples
    --------
    >>> import requests     # To install: pip install requests
    >>> response = get_presigned_upload_url(
    ...    bucket='myBucket',
    ...    key='myData/myFile.data',
    ... )
    >>> with open('myFile.data', 'rb') as f:
    ...    files = {'file': ('myFile.data', f)}
    ...    http_response = requests.post(response['url'], data=response['fields'], files=files)

    """
    if key is None:
        raise AttributeError("Key is required.")

    session = boto3.session.Session(**aws_auth)
    s3 = session.client("s3")

    try:
        response = s3.generate_presigned_post(
            Bucket=bucket,
            Key=Path(key).as_posix(),
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expiration,
        )
    except Exception as error:
        raise error

    return response
