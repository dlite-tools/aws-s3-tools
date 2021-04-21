Introduction
============

AWS S3 Tools is a Python package to make it easier to deal with S3 objects, where you can:

- List S3 buckets' content
- Check if S3 objects exist
- Read from S3 objects to Python variables
- Write from Python variables to S3 objects
- Upload from local files to S3
- Download from S3 to local files
- Delete S3 objects
- Move S3 objects

The AWS authentication is done via boto3 package, `click here <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_.

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
        bucket='daniel-ferrari',
        prefix='aws-s3-tools',
        search_str='*.py',
        threads=2,
        folder='s3_tools',
        show_progress=True
    )

.. image:: ./demo.gif
    :alt: Animated GIF with progress bar
