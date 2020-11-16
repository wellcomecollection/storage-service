import datetime

from wellcome_storage_service import IngestNotFound, RequestsOAuthStorageServiceClient


API_VARIANTS = {"stage": "api-stage", "prod": "api"}


def lookup_ingest(ingest_id):
    """
    Looks up an ingest in both APIs, and returns an (API name, ingest ID) tuple.
    prod and staging API, and returns a tuple (API name, API URL, ingest_data) if
    it finds the ingest -- or IngestNotFound if not.
    """
    api_variants = {"stage": "api-stage", "prod": "api"}

    for api_name, api_host in API_VARIANTS.items():
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


def get_latest_version_of(*, api_url, space, external_identifier):
    """
    Gets the latest version of a bag.
    """
    client = RequestsOAuthStorageServiceClient.from_path(api_url=api_url)

    bag = client.get_bag(space=space, external_identifier=external_identifier)
    return bag["version"]
