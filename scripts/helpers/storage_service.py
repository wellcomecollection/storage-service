import datetime

from wellcome_storage_service import IngestNotFound, prod_client, staging_client


def lookup_ingest(ingest_id):
    """
    Looks up an ingest in both APIs

    If it finds an ingest, it returns a tuple:

        (API name, API client, ingest_data)

    If it doesn't find an ingest, it returns IngestNotFound.

    """
    api_variants = {"staging": staging_client(), "prod": prod_client()}

    for api_name, client in api_variants.items():
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
            return api_name, client, ingest_data

    raise IngestNotFound
