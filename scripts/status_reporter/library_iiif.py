# -*- encoding: utf-8

from contextlib import closing
import random

import requests


class LibraryIIIF:
    """
    Class for fetching manifests from the library site.

    """

    def __init__(
        self,
        stage_url="https://library-uat.wellcomelibrary.org",
        prod_url="https://wellcomelibrary.org",
    ):

        self.stage_url = stage_url
        self.prod_url = prod_url

        self.path_mask = "iiif/{0}/manifest?cachebust=" + str(random.randint(0, 1000))

        self.session = requests.Session()
        self.http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=2, pool_maxsize=25, pool_block=True
        )

        self.session.mount("https://", self.http_adapter)

    def _request_json(self, url):
        r = self.session.get(url, verify=False)

        with closing(r) as response:
            response.raise_for_status()

            return response.json()

    def stage(self, bnumber):
        url = f"{self.stage_url}/{self.path_mask}".format(bnumber)
        return self._request_json(url)

    def prod(self, bnumber):
        url = f"{self.prod_url}/{self.path_mask}".format(bnumber)
        return self._request_json(url)
