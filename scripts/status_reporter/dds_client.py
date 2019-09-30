from contextlib import closing
import requests

class DDSClient:

    def __init__(self, start_ingest_url, item_query_url, connection_pool_size=1):
        self.start_ingest_url = start_ingest_url
        self.item_query_url = item_query_url

        self.session = requests.Session()
        self.http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,
            pool_maxsize=25,
            pool_block=True
        )

        self.https_adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,
            pool_maxsize=25,
            pool_block=True
        )

        self.session.mount('https://', self.http_adapter)
        self.session.mount('http://', self.https_adapter)

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


    def ingest(self, bnumber):
        url = self.start_ingest_url.format(bnumber)
        ingest_result = self._request_json(url)

        if ingest_result['status'] is "ok":
            return "requested"
        else:
            print(ingest_result)
            return "failed_requested"

    def status(self, bnumber):
        url = self.item_query_url.format(bnumber)
        status_result = self._request_json(url)

        result = None

        if(status_result['status'] is 'ok'):
            waiting = status_result['body']['Waiting']
            finished = status_result['body']['Finished']

            if waiting and finished:
                raise Exception(f"Impossible status for {bnumber}, waiting AND finished!")
            elif waiting:
                result = 'waiting'
            elif finished:
                result = 'finished'
            else:
                result = 'unknown'

        elif(status_result['status'] is 'not_found'):
            result = 'not_found'
        else:
            result = 'unknown'

        return {
            'bnumber': bnumber,
            'status': result
        }