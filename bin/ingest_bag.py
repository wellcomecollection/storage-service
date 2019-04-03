#!/usr/bin/env python3
# -*- encoding: utf-8
"""
Create a request to ingest a bag into storage

Usage: ingest_bag.py <BAG> [--oauth-credentials=<OAUTH_CREDENTIALS>] [--bucket=<BUCKET_NAME>] [--storage-space=<SPACE_NAME>]  [--api=(prod|stage)]
       ingest_bag.py -h | --help

Arguments:
    BAG                    Path to BagIt locations to ingest

Examples:
    ingest_bag.py b22454408.zip

Options:
    --oauth-credentials=<OAUTH_CREDENTIALS> The location of the oauth credentials
                                            [default: ~/.wellcome-storage/oauth-credentials.json]
    --bucket=<BUCKET_NAME>                  The S3 bucket containing the bags.
                                            [default: wellcomecollection-storage-ingests]
    --storage-space=<SPACE_NAME>            The space to use when storing the bag
                                            [default: test]
    --api=(prod|stage)                      The ingests API endpoint to use
                                            [default: stage]
    -h --help                               Print this help message

OAuth details:
  Credentials are supplied in a file (default ~/.wellcome-storage/oauth-credentials.json) with the following Json

  {
    "token_url": "https://auth.wellcomecollection.org/oauth2/token",
    "client_id": "YOUR-CLIENT-ID",
    "client_secret": "YOUR-CLIENT-SECRET"
  }
"""

import json
import os

import docopt
from wellcome_storage_service import StorageServiceClient


def main():
    args = docopt.docopt(__doc__)

    bag_path = args["<BAG>"]
    space_id = args["--storage-space"]

    oauth_filepath = os.path.expanduser(args["--oauth-credentials"])
    oauth_details = json.load(open(oauth_filepath))

    if args["--api"] == "stage":
        api_url = "https://api-stage.wellcomecollection.org/storage/v1"
    else:
        api_url = "https://api.wellcomecollection.org/storage/v1"

    sess = StorageServiceClient(
        api_url=api_url,
        client_id=oauth_details["client_id"],
        client_secret=oauth_details["client_secret"],
        token_url=oauth_details["token_url"],
    )

    location = sess.create_s3_ingest(
        space_id=space_id, s3_bucket=args["--bucket"], s3_key=bag_path
    )

    print(location)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        import sys

        sys.exit(1)
