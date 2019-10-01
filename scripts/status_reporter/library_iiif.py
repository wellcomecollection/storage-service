from contextlib import closing

import requests


class LibraryIIIF:
    def __init__(
        self,
        stage_url = 'https://library-uat.wellcomelibrary.org',
        prod_url = 'https://wellcomelibrary.org'
    ):

        self.stage_url = 'https://library-uat.wellcomelibrary.org'
        self.prod_url = 'https://wellcomelibrary.org'

        self.path_mask = 'iiif/{0}/manifest'

        self.session = requests.Session()
        self.http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=2,
            pool_maxsize=25,
            pool_block=True
        )

        self.session.mount('https://', self.http_adapter)

    def _request_json(self, url):
        notes = None
        status = None
        body = None

        try:
            r = self.session.get(url, verify=False)

            with closing(r) as response:
                if response.status_code == 200:
                    body = response.json()
                    status = 'ok'
                elif response.status_code == 404:
                    status = 'not_found'
                else:
                    status = 'not_ok'
                    notes = str(response.status_code)

        except ValueError as e:
            status = 'bad_json'
            notes = str(e)

        except Exception as e:
            status = 'unknown_error'
            notes = str(e)

        return {
            'body': body,
            'status': status,
            'notes': notes
        }


    def stage(self, bnumber):
        url = f"{self.stage_url}/{self.path_mask}".format(bnumber)
        return self._request_json(url)

    def prod(self, bnumber):
        url = f"{self.prod_url}/{self.path_mask}".format(bnumber)
        return self._request_json(url)