# -*- encoding: utf-8 -*-

import os

from setuptools import find_packages, setup


def local_file(name):
    return os.path.relpath(os.path.join(os.path.dirname(__file__), name))


SOURCE = local_file("src")
README = local_file("README.rst")


# Assignment to placate pyflakes. The actual version is from the exec that
# follows.
__version__ = None

with open(local_file("src/wellcome_storage_service/version.py")) as o:
    exec(o.read())

assert __version__ is not None


setup(
    name="wellcome_storage_service",
    packages=find_packages(SOURCE),
    package_dir={"": SOURCE},
    version=__version__,
    install_requires=["requests_oauthlib>=1.2.0,<2"],
    extras_require={"s3": ["boto3>=1.9.253,<2"]},
    description="A client for the Wellcome Storage Service",
    long_description=open(README).read(),
    author="Wellcome Trust (Digital Platform Team)",
    author_email="wellcomedigitalplatform@wellcome.ac.uk",
    url="https://github.com/wellcometrust/storage_service",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
)
