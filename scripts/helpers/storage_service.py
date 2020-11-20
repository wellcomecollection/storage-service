import datetime

from wellcome_storage_service import IngestNotFound, RequestsOAuthStorageServiceClient


def lookup_ingest(ingest_id):
    """
    Looks up an ingest in both APIs

    If it finds an ingest, it returns a tuple:

        (API name, API URL, ingest_data)

    If it doesn't find an ingest, it returns IngestNotFound.

    """
    api_variants = {"staging": "api-stage", "prod": "api"}

    for api_name, api_host in api_variants.items():
        api_url = f"https://{api_host}.wellcomecollection.org/storage/v1"
        client = RequestsOAuthStorageServiceClient.from_path(api_url=api_url)

        try:
            ingest = client.get_ingest(ingest_id)
        except IngestNotFound:
            pass
        else:
            ingest_data = {
                "space": ingest["space"]["id"],
                "external_identifier": ingest["bag"]["info"]["externalIdentifier"],
                "version": ingest["bag"]["info"]["version"],
                "date_created": datetime.datetime.strptime(
                    ingest["createdDate"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
            }
            return api_name, api_url, ingest_data

        raise IngestNotFound
