import datetime

import boto3
import httpx


secrets_client = boto3.client("secretsmanager")


def get_secret(secret_id):
    """
    Get a Secret from Secrets Manager.
    """
    return secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]


def get_es_client():
    """
    Get an instance of httpx.Client authenticated to the reporting cluster.
    """
    es_host = get_secret("storage_service_reporter/es_host")
    es_port = get_secret("storage_service_reporter/es_port")
    es_user = get_secret("storage_service_reporter/es_user")
    es_pass = get_secret("storage_service_reporter/es_pass")

    return httpx.Client(
        base_url=f"https://{es_host}:{es_port}",
        auth=(es_user, es_pass)
    )


def _parse_date(d):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ")


def _parse_ingest_from_hit(hit):
    source = hit["_source"]
    ingest = {
        "id": source["id"],
        "space": source["space"]["id"],
        "status": source["status"]["id"],
        "externalIdentifier": source["bag"]["info"]["externalIdentifier"],
        "version": source["bag"]["info"].get("version"),
        "createdDate": _parse_date(source["createdDate"]),
        "events": [
            {
                "description": ev["description"],
                "createdDate": _parse_date(ev["createdDate"])
            } for ev in source.get("events", [])
        ],
    }

    try:
        ingest["lastModifiedDate"] = _parse_date(source["lastModifiedDate"])
    except (KeyError, TypeError):
        ingest["lastModifiedDate"] = ingest["createdDate"]

    return ingest


def get_interesting_ingests(es_client, *, index_name, days_to_fetch):
    """
    Get ingests that are "interesting".  This could mean:

    *   it was updated recently
    *   it's still processing

    It tries to highlight ingests that might need attention, e.g. failures or
    stalled ingests.

    """
    today = datetime.datetime.now()

    start = today - datetime.timedelta(days=days_to_fetch)

    # Did we find everything we were looking for?  It's possible we might
    found_everything = True

    # Ingests we want to return
    ingests = {}

    # We make three requests to Elasticsearch, to maximise the chance of finding
    # ingests that might need more attention:
    #
    #   * everything modified in the last N days
    #   * everything modified in the last N days which hasn't succeeded
    #   * everything which hasn't completed
    #
    # A single ES request returns up to 10000 documents; if more than that have
    # been updated (e.g. we've just done a big migration), we want to flag that
    # fact and try to find the most interesting stuff.
    filters = [
        [{"range": {"lastModifiedDate": {"gte": start.strftime("%Y-%m-%d")}}}],
        [{
            "range": {"lastModifiedDate": {"gte": start.strftime("%Y-%m-%d")}}},{
            "terms": {"status.id": ["processing", "accepted", "failed"]},
        }],
        [{"terms": {"status.id": ["processing", "accepted"]}}],
    ]

    for filter_clause in filters:
        resp = es_client.request(
            method="GET",
            url=f"/{index_name}/_search",
            json={
                "query": {"bool": {"filter": filter_clause}},
                "_source": [
                    "id",
                    "lastModifiedDate",
                    "space.id",
                    "status.id",
                    "bag.info.externalIdentifier",
                    "bag.info.version",
                    "createdDate",
                    "events.createdDate",
                    "events.description",
                ],
                # We can retrieve up to 10000 documents in one request.
                "size": 10000,
                "sort": [{"id": "desc"}],
            },
        )

        # We use a dict to de-duplicate result.  The same ingest may appear
        # multiple times in this response -- that's not an issue.
        for hit in resp.json().get("hits", {}).get("hits", []):
            ingests[hit["_id"]] = _parse_ingest_from_hit(hit)

        # If there are more than 10000 results, that suggests there are more
        # that we didn't get.  Flag this to the caller.
        try:
            if resp.json()["hits"]["total"]["value"] >= 10000:
                found_everything = False
        except KeyError:
            # Nothing in hits, no results to find
            pass

    return {"ingests": ingests.values(), "found_everything": found_everything}
