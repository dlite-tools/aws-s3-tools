# AWS S3 Tools

![MIT License](https://img.shields.io/pypi/l/aws-s3-tools)
![Package Version](https://img.shields.io/pypi/v/aws-s3-tools)
![Python Version](https://img.shields.io/pypi/pyversions/aws-s3-tools)

- [Install](#install)
- [TO-DO](#to-do)

---

Python package for AWS S3 functionalities to have a clear code around `boto3` methods. The authentication is done by boto3 package, [to know more](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

---

## Install

You can use `pip` to install:

```shell
pip3 install aws-s3-tools
```

You can install directly from Github:

```shell
pip3 install --user git+https://github.com/FerrariDG/aws-s3-tools.git
```

Or you can clone the repository:

```shell
pip3 install --user <full path to>/aws-s3-tools
```

--

## TO-DO

- Add automatic doc generation from docstrings
- S3 Functions:
  - Improve error handling by creating Exceptions
  - Add functions to move objects
