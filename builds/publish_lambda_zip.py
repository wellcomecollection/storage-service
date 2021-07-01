#!/usr/bin/env python3
"""
Build a deployment ZIP for a Lambda, and upload it to Amazon S3.

Usage:
  publish_lambda_zip.py <PATH> --bucket=<BUCKET> --key=<KEY>
  publish_lambda_zip.py -h | --help

Options:
  <PATH>                Path to the source code for the Lambda
  --bucket=<BUCKET>     Name of the Amazon S3 bucket
  --key=<KEY>           Key for the upload ZIP file
"""

import os
import shutil
import subprocess
import tempfile
import zipfile

import boto3
import docopt


def cmd(*args):
    return subprocess.check_output(list(args)).decode("utf8").strip()


def git(*args):
    return cmd("git", *args)


ROOT = git("rev-parse", "--show-toplevel")

ZIP_DIR = os.path.join(ROOT, ".lambda_zips")


def create_zip(src, dst):
    """
    Zip a source directory into a target directory.

    Based on https://stackoverflow.com/a/14569017/1558022
    """
    with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
        abs_src = os.path.abspath(src)
        for dirname, subdirs, files in os.walk(src):
            for filename in files:
                if filename.startswith("."):
                    continue
                absname = os.path.abspath(os.path.join(dirname, filename))
                arcname = absname[len(abs_src) + 1 :]
                zf.write(absname, arcname)


def build_lambda_local(path, name):
    """
    Construct a Lambda ZIP bundle on the local disk.  Returns the path to
    the constructed ZIP bundle.

    :param path: Path to the Lambda source code.
    """
    print(f"*** Building Lambda ZIP for {name}")
    target = tempfile.mkdtemp()

    # Copy all the associated source files to the Lambda directory.
    src = os.path.join(path, "src")
    for f in os.listdir(src):
        if f.startswith(
            (
                # Required for tests, but unneeded in our prod images
                "test_",
                "__pycache__",
                "docker-compose.yml",
                # Hidden files
                ".",
                # Virtualenv
                "venv",
                # Required for installation, not for our prod Lambdas
                "requirements.in",
                "requirements.txt",
            )
        ):
            continue

        try:
            shutil.copy(
                src=os.path.join(src, f), dst=os.path.join(target, os.path.basename(f))
            )
        except IsADirectoryError:
            shutil.copytree(
                src=os.path.join(src, f), dst=os.path.join(target, os.path.basename(f))
            )

    # Now install any additional pip dependencies.
    for reqs_file in [
        os.path.join(path, "requirements.txt"),
        os.path.join(path, "src", "requirements.txt"),
    ]:
        if os.path.exists(reqs_file):
            print(f"*** Installing dependencies from {reqs_file}")
            subprocess.check_call(
                ["pip3", "install", "--requirement", reqs_file, "--target", target]
            )
        else:
            print(f"*** No requirements.txt found at {reqs_file}")

    print(f"*** Creating zip bundle for {name}")
    os.makedirs(ZIP_DIR, exist_ok=True)
    src = target
    dst = os.path.join(ZIP_DIR, name)
    create_zip(src=src, dst=dst)
    return dst


if __name__ == "__main__":
    args = docopt.docopt(__doc__)

    path = args["<PATH>"]
    key = args["--key"]
    bucket = args["--bucket"]

    client = boto3.client("s3")

    name = os.path.basename(key)
    filename = build_lambda_local(path=path, name=name)

    client.upload_file(Bucket=bucket, Filename=filename, Key=key)
