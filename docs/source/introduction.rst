Introduction
============

AWS S3 Tools is a Python package to make it easier to interact with S3 objects, where you can:

- List S3 bucket content
- Check if an S3 object exists
- Download/upload S3 objects to/from local files
- Read/write S3 objects into/from Python variables
- Delete/Move S3 objects

The AWS S3 authentication is done via boto3 package, via environment variables, aws config file, or parameters.
All S3 objects functions, in this package, have the option to set AWS Session authentication by passing the following dictionary on the `aws_auth` parameter, with the schema below (not all field are required).
To understand more about AWS authentication mechanism, `read boto3 documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_.

.. code-block:: python
    aws_auth = {
        'region_name': 'REGION',
        'aws_access_key_id': 'ACCESS_KEY',
        'aws_secret_access_key': 'SECRET_KEY',
        'aws_session_token': 'SESSION_TOKEN',
        'profile_name': 'PROFILE_NAME'
    }

Installation
------------

You can install AWS S3 Tools from PyPi with `pip` or your favorite package manager::

    pip install aws-s3-tools

Add the ``-U`` switch to update to the current version, if AWS S3 Tools is already installed.

If you want to use the **progress bar** feature when downloading or uploading, you need to install an extra dependency::

    pip install aws-s3-tools[progress]


Usage
-----

Simple example:

.. code-block:: python

    from s3_tools import object_exists

    if object_exists("my-bucket", "s3-prefix/object.data"):
        # Do magic
        pass
    else:
        print("Object not found")

Using the progress bar:

.. code-block:: python

    from s3_tools import upload_folder_to_prefix

    result = upload_folder_to_prefix(
        bucket='dlite-tools',
        prefix='aws-s3-tools',
        search_str='*.py',
        threads=2,
        folder='s3_tools',
        show_progress=True
    )

.. image:: ./demo.gif
    :alt: Animated GIF with progress bar
