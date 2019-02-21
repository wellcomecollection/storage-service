# -*- encoding: utf-8

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
from xml_help import expand, namespaces

# keep track of these to ensure no collisions in Multiple Manifestations
ALTO_KEYS = set()
OBJECT_KEYS = set()


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
                return

        # If we've tried all the possibilities and none of them worked,
        # we haven't found the METS file.  Give up!
        message = "Unable to find source for {0}: {1}".format(b_number, source)
        raise RuntimeError(message)


def process_alto(root, bag_details, alto, skip_file_download):
    # TODO use the alto map to verify
    b_number = bag_details["b_number"]
    logging.debug("Collecting ALTO for %s", b_number)
    alto_file_group = root.find("./mets:fileSec/mets:fileGrp[@USE='ALTO']", namespaces)

    if alto_file_group is None:
        logging.debug("No ALTO for " + b_number)
        return

    source_bucket = None

    if not settings.READ_METS_FROM_FILESHARE:
        source_bucket = aws.get_s3().Bucket(settings.METS_BUCKET_NAME)

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
            download_s3_object(
                b_number=b_number,
                source_bucket=source_bucket,
                source=source,
                destination=destination,
            )


def get_flattened_destination(file_element, keys, folder, bag_details):
    locator = file_element[0]
    current_location = locator.get(expand("xlink", "href"))
    # we want to flatten the multiple manifestation directory structure
    # so that all the ALTOs/objects are in the same directory
    file_name = current_location.split("/")[-1]
    keys.add(file_name)  # let this raise error if duplicate
    desired_relative_location = "{0}/{1}".format(folder, file_name)
    locator.set(expand("xlink", "href"), desired_relative_location)
    logging.debug("updated path in METS to " + desired_relative_location)
    # the local temp assembly area
    destination = os.path.join(bag_details["directory"], folder, file_name)
    bag_assembly.ensure_directory(destination)
    return current_location, destination


def process_assets(root, bag_details, assets, skip_file_download):
    logging.debug("Collecting assets for " + bag_details["b_number"])

    chunk_size = 1024 * 1024

    asset_file_group = root.find(
        "./mets:fileSec/mets:fileGrp[@USE='OBJECTS']", namespaces
    )
    for file_element in asset_file_group:
        current_location, destination = get_flattened_destination(
            file_element, OBJECT_KEYS, "objects", bag_details
        )
        # current_location is not used for objects - they're not where
        # the METS says they are! They are in Preservica instead.
        # but, when bagged, they _will_ be where the METS says they are.
        summary = assets[file_element.get("ID")]
        tech_md = summary["tech_md"]
        checksum = file_element.get("CHECKSUM")
        file_element.attrib.pop("CHECKSUM")  # don't need it now
        pres_uuid = tech_md["uuid"]
        logging.debug("Need to determine where to get {0} from.".format(pres_uuid))

        if skip_file_download:
            logging.debug("Skipping processing file {0}".format(pres_uuid))
            continue

        image_info = dlcs.get_image(pres_uuid)
        origin = image_info.get("origin", None)
        logging.debug("DLCS reports origin " + str(origin))
        # if the origin is wellcomelibrary.org, the object is LIKELY to be in the DLCS's
        # storage bucket. So we should try that first, then fall back to the wellcomelibrary
        # origin (using the creds) if for whatever reason it isn't in the DLCS bucket.
        origin_info = storage.analyse_origin(origin, pres_uuid)
        bucket_name = origin_info["bucket_name"]
        asset_downloaded = False
        if bucket_name is not None:
            source_bucket = aws.get_s3().Bucket(bucket_name)
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
                alt_key = origin_info["alt_key"]
                if ce.response["Error"]["Code"] == "NoSuchKey" and alt_key is not None:
                    logging.debug(
                        "key {0} not found, trying alternate key: {1}".format(
                            bucket_key, alt_key
                        )
                    )
                    source_bucket.download_file(alt_key, destination)
                    asset_downloaded = True
                    # allow error to throw

        web_url = origin_info["web_url"]
        if not asset_downloaded and web_url is not None:
            # This will probably fail, if the DLCS hasn't got it.
            # But it is the only way of getting restricted files out.
            user, password = settings.DDS_API_KEY, settings.DDS_API_SECRET
            # This is horribly slow, why?
            message = "Try to fetch this from Preservica directly, via DDS, at {0}".format(
                origin_info["web_url"]
            )
            logging.debug(message)
            resp = requests.get(
                origin_info["web_url"], auth=(user, password), stream=True
            )
            if resp.status_code == 200:
                with open(destination, "wb") as f:
                    for chunk in resp.iter_content(chunk_size):
                        f.write(chunk)
                asset_downloaded = True

        message = "Unable to find asset {0}".format(pres_uuid)
        assert asset_downloaded, message

        logging.debug("TODO: doing checksums on " + destination)
        logging.debug("validate " + checksum)
