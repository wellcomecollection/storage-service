import json
import os

from wellcome_storage_service import StorageServiceClient


class StorageClient:
    def __init__(self, api_url="https://api.wellcomecollection.org/storage/v1"):
        creds_path = os.path.join(
            os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
        )

        oauth_creds = json.load(open(creds_path))

        self.client = StorageServiceClient(
            api_url=api_url,
            client_id=oauth_creds["client_id"],
            client_secret=oauth_creds["client_secret"],
            token_url=oauth_creds["token_url"],
        )

    def get(self):
        return self.client
