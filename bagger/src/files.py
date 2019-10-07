# -*- encoding: utf-8

import re

import logging
import aws
import shutil
import os
import requests
from botocore.exceptions import ClientError
import settings
import bag_assembly
import dlcs
import storage
import tech_md
from xml_help import expand, namespaces

import boto3

s3_client = boto3.client("s3")

# keep track of these to ensure no collisions in Multiple Manifestations
ALTO_KEYS = set()
OBJECT_KEYS = set()

# For bagging posters when running on AWS
POSTER_CANDIDATES = [".mpg", ".mpeg", ".mp3", ".wav", ".mp4"]
POSTER_SOURCE = "https://wellcomelibrary.org/posterimages/{0}.{1}"


def download_s3_object(b_number, source_bucket, source, destination):
    try:
        source_bucket.download_file(source, destination)
    except ClientError:
        logging.debug("Checking for case mismatch in ALTO file name")

        # Sometimes the S3 objects have a case mismatch between the actual object
        # and the ALTO reference.  In that case, we should try some alternative
        # keys (by fiddling with the case) to see if one of the alternatives works.
        #
        # We use 'set()' to avoid trying the same key multiple times.
        #
        alternative_bnums = set(
            [
                b_number.replace("x", "X"),
                b_number.replace("b", "B"),
                b_number.replace("x", "X").replace("b", "B"),
            ]
        ) - set([b_number])

        original_filename = os.path.basename(source)

        for alt_bnum in alternative_bnums:
            alt_filename = original_filename.replace(b_number, alt_bnum)
            alt_source = os.path.join(os.path.dirname(source), alt_filename)
            logging.debug("Retrying from %s to %s", alt_source, destination)

            try:
                source_bucket.download_file(alt_source, destination)
            except ClientError as err:
                logging.debug("Unsuccessful downloading %s, %r", alt_source, err)
            else:
                return True

        # If we've tried all the possibilities and none of them worked,
        # we haven't found the METS file.  Give up!
        return False


def process_alto(root, bag_details, alto, skip_file_download):
    # TODO use the alto map to verify
    b_number = bag_details["b_number"]
    logging.debug("Collecting ALTO for %s", b_number)
    alto_file_group = root.find("./mets:fileSec/mets:fileGrp[@USE='ALTO']", namespaces)

    if alto_file_group is None:
        logging.debug("No ALTO for %s", b_number)
        return

    source_bucket = None

    if not settings.READ_METS_FROM_FILESHARE:
        source_bucket = aws.get_bucket(settings.METS_BUCKET_NAME)

    missing_altos = []

    for file_element in alto_file_group:
        current_location, destination = get_flattened_destination(
            file_element, ALTO_KEYS, "alto", bag_details
        )

        if skip_file_download:
            logging.debug(
                "Skipping fetch of alto from %s to %s", current_location, destination
            )
            continue

        if settings.READ_METS_FROM_FILESHARE:
            # Not likely to be used much, only for running against Windows file share
            source = os.path.join(
                settings.METS_FILESYSTEM_ROOT,
                bag_details["mets_partial_path"],
                current_location,
            )
            logging.debug("Copying alto from %s to %s", source, destination)
            shutil.copyfile(source, destination)
        else:
            source = bag_details["mets_partial_path"] + current_location
            logging.debug("Downloading S3 ALTO from %s to %s", source, destination)
            downloaded = download_s3_object(
                b_number=b_number,
                source_bucket=source_bucket,
                source=source,
                destination=destination,
            )
            if not downloaded:
                message = "Unable to find source for {0}: {1}".format(b_number, source)
                logging.debug(message)
                missing_altos.append(source)

        missing_count = len(missing_altos)
        if missing_count > 0:
            message = {
                "identifier": b_number,
                "summary": "{0} has {1} missing ALTO files.".format(
                    b_number, missing_count
                ),
                "data": missing_altos,
            }
            aws.log_warning("missing_altos", message)


def get_flattened_destination(file_element, keys, folder, bag_details):
    locator = file_element[0]
    current_location = locator.get(expand("xlink", "href"))
    # we want to flatten the multiple manifestation directory structure
    # so that all the ALTOs/objects are in the same directory
    file_name = current_location.split("/")[-1]
    keys.add(file_name)  # let this raise error if duplicate
    desired_relative_location = "{0}/{1}".format(folder, file_name)
    locator.set(expand("xlink", "href"), desired_relative_location)
    logging.debug("updated path in METS to %s", desired_relative_location)
    # the local temp assembly area
    destination = os.path.join(bag_details["directory"], folder, file_name)
    bag_assembly.ensure_directory(destination)
    return current_location, destination


def process_assets(root, bag_details, assets, tech_md_files, skip_file_download):
    logging.debug("Collecting assets for %s", bag_details["b_number"])
    asset_file_group = root.find(
        "./mets:fileSec/mets:fileGrp[@USE='OBJECTS']", namespaces
    )

    # TODO - keep track of the asset IDs that we fetch.
    # Compare with the tech_md_files passed.
    # if there is a mismatch, download the techMD files
    # log to the warnings bucket
    # write to status table

    structmap_uuids_downloaded = []

    for file_element in asset_file_group:
        _current_location, destination = get_flattened_destination(
            file_element, OBJECT_KEYS, "objects", bag_details
        )
        # current_location is not used for objects - they're not where
        # the METS says they are! They are in Preservica instead.
        # but, when bagged, they _will_ be where the METS says they are.

        # These assets only know about the structmap-asserted assets.
        # If there are tech_md defined assets that aren't in the structmap,
        # they won't be here but will be in tech_md_files
        summary = assets[file_element.get("ID")]
        tech_md_for_structmap_asset = summary["tech_md"]
        checksum = file_element.get("CHECKSUM")
        file_element.attrib.pop("CHECKSUM")  # don't need it now
        preservica_uuid = tech_md_for_structmap_asset["uuid"]
        logging.debug("Need to determine where to get %s from.", preservica_uuid)

        if skip_file_download:
            logging.debug("Skipping processing file %s", preservica_uuid)
            # We assume that the download would always have been a success
            structmap_uuids_downloaded.append(preservica_uuid)
            continue

        fetch_attempt = try_to_download_asset(preservica_uuid, destination)
        if fetch_attempt["succeeded"]:
            structmap_uuids_downloaded.append(preservica_uuid)
            # ? Make sure what just got matches what the METS file says
            logging.debug("TODO: doing checksums on %s", destination)
            logging.debug("validate %s", checksum)
        else:
            # If we can't get hold of an asset mentioned in the structmap,
            # it's definitely an error that needs attention
            raise RuntimeError(fetch_attempt["message"])

    # Now see if there are files we didn't collect in that set of downloads
    # These are files mentioned in tech_md but not in the structmap
    # These are not considered errors, but they definitely need attention
    # TODO - see how many errors we get this way and decide whether a missing
    # techMd-only file (can't obtain from anywhere) is still an error

    tech_md_mismatch_warnings = []
    missing_from_preservica = []
    for tech_md_file in tech_md_files:
        preservica_uuid = tech_md_file["preservica_id"]
        if preservica_uuid not in structmap_uuids_downloaded:
            # double sanity check - we should have picked this up in the techMd restructure
            assert tech_md_file.get("warning", None) is not None
            logging.debug(tech_md_file["warning"])
            tech_md_mismatch_warnings.append(tech_md_file["warning"])

            if skip_file_download:
                continue

            # we've recorded the warning, now try to get the file
            folder = "objects"
            filename = tech_md_file["filename"]
            destination = os.path.join(bag_details["directory"], folder, filename)
            bag_assembly.ensure_directory(destination)
            fetch_attempt = try_to_download_asset(preservica_uuid, destination)
            if fetch_attempt["succeeded"]:
                logging.debug("successfully fetched %s - %s", preservica_uuid, filename)
            else:
                missing_from_preservica.append(
                    {"preservica_uuid": preservica_uuid, "filename": filename}
                )
                logging.debug("Unable to fetch %s - %s", preservica_uuid, filename)

    mismatch_count = len(tech_md_mismatch_warnings)
    if mismatch_count > 0:
        b_number = bag_details["b_number"]
        message = {
            "identifier": b_number,
            "summary": "{0} has {1} AMD/techMD mismatches.".format(
                b_number, mismatch_count
            ),
            "data": tech_md_mismatch_warnings,
        }
        if len(missing_from_preservica) > 0:
            message["missing_from_preservica"] = missing_from_preservica

        aws.log_warning("amd_mismatch", message)


def try_to_download_asset(preservica_uuid, destination):
    # The DLCS might return an empty image here, if it doesn't have it
    image_info = dlcs.get_image(preservica_uuid)
    # ... which won't have an origin
    origin = image_info.get("origin", None)
    logging.debug("DLCS reports origin %s", origin)

    # if the origin is wellcomelibrary.org, the object is LIKELY to be in the DLCS's
    # storage bucket. So we should try that first, then fall back to the wellcomelibrary
    # origin (using the creds) if for whatever reason it isn't in the DLCS bucket.
    origin_info = storage.analyse_origin(origin, preservica_uuid)
    bucket_name = origin_info["bucket_name"]
    asset_downloaded = False
    if bucket_name is not None:
        source_bucket = aws.get_bucket(bucket_name)
        bucket_key = origin_info["bucket_key"]
        logging.debug(
            "Downloading object from bucket {0}/{1} to {2}".format(
                bucket_name, bucket_key, destination
            )
        )
        try:
            source_bucket.download_file(bucket_key, destination)
            asset_downloaded = True
        except ClientError as ce:
            alt_key = origin_info.get("alt_key", None)
            if ce.response["Error"]["Code"] == "404" and alt_key is not None:
                logging.debug(
                    "key {0} not found, trying alternate key: {1}".format(
                        bucket_key, alt_key
                    )
                )
                try:
                    source_bucket.download_file(alt_key, destination)
                    asset_downloaded = True
                except ClientError:
                    logging.info(
                        "{0} should have been in {1} but wasn't".format(
                            alt_key, bucket_key
                        )
                    )

    web_url = origin_info["web_url"]
    if not asset_downloaded and web_url is not None:
        asset_downloaded = fetch_from_wlorg(
            web_url=web_url, destination=destination, retry_attempts=3
        )

    # TODO: this message could give a more detailed error report
    if asset_downloaded:
        return {"succeeded": True}
    else:
        return {
            "succeeded": False,
            "message": "Unable to find asset {0}".format(preservica_uuid),
        }


def fetch_from_wlorg(web_url, destination, retry_attempts):
    # First, look to see if the object exists in the bagger asset cache.
    # This relieves the load on DDS and reduces the flakiness caused
    # by SSL errors.
    #
    # We use the Preservica GUID as the key because there should be
    # a 1:1 correspondence between these URLs and the Preservica GUIDs,
    # and each unique collection of bytes in Preservica should have
    # its own GUID.
    #
    m = re.match(
        r'^https://wellcomelibrary\.org/service/asset/(?P<guid>[0-9a-f\-]+)$',
        web_url)
    if m is not None:
        preservica_uuid = m.group("guid")
    else:
        raise ValueError("Don't know how to get Preservica GUID from {web_url}")

    try:
        logging.debug(
            "Looking for cached asset at s3://%s/%s",
            settings.CACHE_BUCKET,
            preservica_uuid,
        )
        s3_client.download_file(
            Bucket=settings.CACHE_BUCKET, Key=preservica_uuid, Filename=destination
        )
        return True
    except ClientError:
        pass

    # This will probably fail, if the DLCS hasn't got it.
    # But it is the only way of getting restricted files out.
    user, password = settings.DDS_API_KEY, settings.DDS_API_SECRET
    message = "Try to fetch this from Preservica directly, via DDS, at {0}".format(
        web_url
    )
    logging.debug(message)
    chunk_size = 1024 * 1024
    download_err = None
    for _ in range(retry_attempts):
        try:
            # This is horribly slow, why?
            logging.info(message)  # remove me!
            resp = requests.get(web_url, auth=(user, password), stream=True)
            if resp.status_code == 200:
                with open(destination, "wb") as f:
                    for chunk in resp.iter_content(chunk_size):
                        f.write(chunk)

                s3_client.upload_file(
                    Bucket=settings.CACHE_BUCKET,
                    Key=preservica_uuid,
                    Filename=destination,
                )

                return True
            else:
                logging.debug("Received HTTP %s for %s", resp.status_code, web_url)
                return False
        except Exception as err:
            download_err = err
            pass
    raise download_err


def check_for_posterimages(root, tech_md_files, bag_details, skip_file_download):
    # Going to try and catch as many AV files as possible without relying
    # on logical structure, but we need to bail out as quickly as possible

    # TODO: spend less time in here by knowing the object is AV by other means
    for tech_md_file in tech_md_files:
        base, ext = os.path.splitext(tech_md_file["filename"])
        if might_have_poster(ext):
            file_name = base + ".jpg"
            destination = os.path.join(bag_details["directory"], "posters", file_name)
            if download_poster(base, destination):
                tech_md.append_poster(root, destination)


def might_have_poster(ext):
    if ext == ".jp2":
        return False
    return ext.lower() in POSTER_CANDIDATES


def download_poster(base, destination):
    for jpgext in ["jpg", "jpeg"]:
        url = POSTER_SOURCE.format(base, jpgext)
        response = requests.get(url)
        if response.status_code == 200:
            bag_assembly.ensure_directory(destination)
            with open(destination, "wb") as f:
                f.write(response.content)
                return True
    return False
