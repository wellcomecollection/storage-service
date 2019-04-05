#!/usr/bin/env python3
# -*- encoding: utf-8
"""
Look up the state of an ingest in the storage API.

Usage: lookup_ingest.py <LOCATION> [--oauth-credentials=<OAUTH_CREDENTIALS>]
       lookup_ingest.py -h | --help

Arguments:
    LOCATION                The location of the ingest supplied in the response from
                            the initial call to /ingests.

Examples:
    ingest_bag.py b22454408.zip

Options:
    --oauth-credentials=<OAUTH_CREDENTIALS> The location of the oauth credentials
                                            [default: ~/.wellcome-storage/oauth-credentials.json]
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

    location_url = args["<LOCATION>"]

    oauth_filepath = os.path.expanduser(args["--oauth-credentials"])
    oauth_details = json.load(open(oauth_filepath))

    if location_url.startswith("https://api-stage"):
        api_url = "https://api-stage.wellcomecollection.org/storage/v1"
    else:
        api_url = "https://api.wellcomecollection.org/storage/v1"

    sess = StorageServiceClient(
        api_url=api_url,
        client_id=oauth_details["client_id"],
        client_secret=oauth_details["client_secret"],
        token_url=oauth_details["token_url"],
    )

    resp = sess.get_ingest_from_location(location_url)
    print(json.dumps(resp, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        import sys

        sys.exit(1)
