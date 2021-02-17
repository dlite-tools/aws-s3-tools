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

The AWS authentication is done via boto3 package, `click here <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_.

Installation
------------

You can install AWS S3 Tools from PyPi with `pip` or your favorite package manager::

    pip install aws-s3-tools

Add the ``-U`` switch to update to the current version, if AWS S3 Tools is already installed.

Usage
-----

.. code-block:: python

    from s3_tools import object_exists

    if object_exists("my-bucket", "s3-prefix/object.data"):
        # Do magic
        pass
    else:
        print("Object not found")
