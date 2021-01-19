#!/usr/bin/env python3
"""
Functions related to registering image batches with DLCS
"""

import functools
import json
import os

import attr
import elasticsearch
import httpx

from elastic_helpers import get_local_elastic_client


CREDENTIALS_PATH = os.path.join(os.environ["HOME"], ".dlcs-cli", "credentials.json")


NO_BATCH_QUERY = {
    "query": {"bool": {"must_not": {"exists": {"field": "dlcs.batch_id"}}}}
}

WITH_BATCH_QUERY = {"query": {"exists": {"field": "dlcs.batch_id"}}}

NOT_SUCCEEDED_QUERY = {
    "query": {
        "bool": {
            "must_not": {"term": {"dlcs.image_successful": True}},
            "must": {"exists": {"field": "dlcs.batch_id"}},
        }
    }
}

ONLY_FAILED_QUERY = {
    "query": {"bool": {"must": {"term": {"dlcs.image_successful": False}}}}
}

ONLY_SUCCEEDED_QUERY = {
    "query": {"bool": {"must": {"term": {"dlcs.image_successful": True}}}}
}


@attr.s
class DlcsSettings:
    key = attr.ib()
    secret = attr.ib()
    customer = attr.ib()
    space = attr.ib()
    api_url = attr.ib(default="https://api.dlcs.io/")
    origin = attr.ib(default="storage-origin")


@attr.s
class RegistrationUpdate:
    miro_id = attr.ib()
    update_doc = attr.ib()


def load_dlcs_settings():
    raw_settings = json.load(open(CREDENTIALS_PATH))
    return DlcsSettings(**raw_settings)


def _build_collection_member(space, file_id, miro_id):
    https_base = "https://s3-eu-west-1.amazonaws.com/wellcomecollection-storage"
    s3_base = "s3://wellcomecollection-storage"

    updated_file_id = file_id.replace(s3_base, https_base)

    if file_id.endswith(".tif"):
        mediaType = "image/tiff"
    else:
        mediaType = "image/jp2"

    return {
        "space": space,
        "origin": updated_file_id,
        "id": miro_id,
        "mediaType": mediaType
    }


def _build_collection(space, registrations):
    collection_members = [
        _build_collection_member(
            space=space, file_id=reg["file_id"], miro_id=reg["miro_id"]
        )
        for reg in registrations
    ]

    return {"@type": "Collection", "member": collection_members}


def register_image_batch(registrations):
    settings = load_dlcs_settings()

    collection_to_register = _build_collection(
        space=settings.space, registrations=registrations
    )

    queue_api_url = f"{settings.api_url}customers/2/queue"

    resp = httpx.post(
        queue_api_url, auth=(settings.key, settings.secret), json=collection_to_register
    )

    return resp.json()


def get_registrations(registrations_index, query):
    local_elastic_client = get_local_elastic_client()

    results = elasticsearch.helpers.scan(
        local_elastic_client, query=query, index=registrations_index
    )

    for result in results:
        yield result["_source"]


@functools.lru_cache()
def get_dlcs_object(url):
    settings = load_dlcs_settings()

    resp = httpx.get(url, auth=(settings.key, settings.secret))

    return resp.json()


def update_registrations(registrations_index, registration_updates):
    local_elastic_client = get_local_elastic_client()
    actions = [
        {
            "_id": update.miro_id,
            "_index": registrations_index,
            "_source": {"doc": update.update_doc},
            "_op_type": "update",
        }
        for update in registration_updates
    ]

    elasticsearch.helpers.bulk(client=local_elastic_client, actions=actions)


def check_batch_successful(batch_id):
    batch = get_dlcs_object(batch_id)

    finished = batch["count"] == batch["completed"]
    no_error = batch["errors"] == 0

    successful = finished and no_error

    return successful


def check_image_successful(image_id):
    image = get_dlcs_object(image_id)

    # The response if the image does not exist is {"success": False}
    image_exists = image.get("success", True)

    if image_exists:
        finished = image["finished"] != ""
        no_error = image["error"] == ""
        successful = finished and no_error

        return successful
    else:
        return False


def get_image_error(image_id):
    image = get_dlcs_object(image_id)
    error = image["error"]

    if error == "":
        error = None

    return error


def get_dlcs_image_id(miro_id):
    settings = load_dlcs_settings()
    customer = settings.customer
    space = settings.space

    return f"{settings.api_url}customers/{customer}/spaces/{space}/images/{miro_id}"
