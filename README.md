# AWS S3 Tools

![MIT License](https://img.shields.io/pypi/l/aws-s3-tools)
![Documentation Status](https://readthedocs.org/projects/aws-s3-tools/badge/?version=latest)
![Package Version](https://img.shields.io/pypi/v/aws-s3-tools)
![Python Version](https://img.shields.io/pypi/pyversions/aws-s3-tools)

AWS S3 Tools is a Python package to make it easier to deal with S3 objects, where you can:

- List S3 buckets' content
- Check if S3 objects exist
- Read from S3 objects to Python variables
- Write from Python variables to S3 objects
- Upload from local files to S3
- Download from S3 to local files
- Delete S3 objects
- Move S3 objects

The AWS authentication is done via boto3 package, [click here to know more about it](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

---

## Installation

You can install AWS S3 Tools from PyPi with `pip` or your favorite package manager:

    pip install aws-s3-tools

Add the ``-U`` switch to update to the current version, if AWS S3 Tools is already installed.

---

## Usage

[The full documentation can be found here](https://aws-s3-tools.readthedocs.io/en/latest/index.html).

    ```python
    from s3_tools import object_exists

    if object_exists("my-bucket", "s3-prefix/object.data"):
        # Do magic
    else:
        print("Object not found")
    ```

---

## Next Steps

- Improve error handling by creating Exceptions

---

## Acknowledgement

The idea from these functions come from an amazing team that I worked with. This repo is a refactor and documentation to make this public to everyone.

Many thanks to:

- [Anabela Nogueira](https://www.linkedin.com/in/abnogueira/)
- [Carlos Alves](https://www.linkedin.com/in/carlosmalves/)
- [João Machado](https://www.linkedin.com/in/machadojpf/)
- [Renato Dantas](https://www.linkedin.com/in/renatomoura/)
- [Ricardo Garcia](https://www.linkedin.com/in/ricardo-g-oliveira/)
- [Tomás Osório](https://www.linkedin.com/in/tomas-osorio/)
