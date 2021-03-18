from setuptools import (
    find_packages,
    setup
)

exclude_packages = ["tests", "tests.*"]

setup(
    name="aws-s3-tools",
    version="0.0.3",
    packages=find_packages(exclude=exclude_packages),
    author="Daniel Ferrari",
    description="AWS S3 tools package.",
    license="MIT",
    keywords="aws s3 tools package",
    url="https://github.com/FerrariDG/aws-s3-tools",
    projects_urls={
        "Source": "https://github.com/FerrariDG/aws-s3-tools",
        "Documentation": "https://aws-s3-tools.readthedocs.io/en/latest/index.html"
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: Freely Distributable",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities"
    ],
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    python_requires='>=3.7.*',
    install_requires=[
        "boto3>=1.16.51",
        "ujson>=4.0.1"
    ]
)
