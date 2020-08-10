#!/usr/bin/env python
"""
The storage service is agnostic about the files it stores.  It only verifies
that you pass it a valid BagIt "bag" -- it's not interested in the contents
of the bag.

This script inspects a digitised bag, and does some sense checking of
the METS XML and the bag contents.  In particular:

-   If the bag is a multiple manifestation, the anchor file (b1234.xml) will refer
    to one or more METS files for the individual volumes (b1234_1.xml, b1234_2.xml, ...)
    Check those METS files are all included in the bag.

-   The METS files refer to ALTO files (XML text, OCR) and JP2 image files.
    Check all of those files are included in the bag.

It only checks the latest version of a bag.

"""

import os
import sys

from lxml import etree
import termcolor

from _aws import get_aws_client
from common import get_storage_client


NAMESPACES = {
    "mets": "http://www.loc.gov/METS/",
    "xlink": "http://www.w3.org/1999/xlink",
}


def info(message):
    print("%s\t%s" % (termcolor.colored("info", "blue"), message))


def abort(message):
    print("%s\t%s" % (termcolor.colored("error", "red"), message))
    sys.exit(1)


def success(message):
    print("%s\t%s" % (termcolor.colored("success", "green"), message))


def get_xml_from_s3(location, path):
    s3 = get_aws_client("s3")

    s3_obj = s3.get_object(
        Bucket=location["bucket"],
        Key=os.path.join(location["path"], path)
    )

    return etree.parse(s3_obj["Body"])


def get_alto_references(tree):
    # Look for blocks of the form:
    #
    #   <mets:fileGrp USE="ALTO">
    #     <mets:file ID="FILE_0001_ALTO" MIMETYPE="application/xml">
    #       <mets:FLocat LOCTYPE="URL" xlink:href="alto/b30354730_0001.xml" />
    #     </mets:file>
    #     ...
    #   </mets:fileGrp>
    #
    return set(
        tree.xpath(
            './/mets:fileGrp[@USE="ALTO"]//mets:FLocat/@xlink:href',
            namespaces=NAMESPACES,
        )
    )


def get_object_references(tree):
    return set(
        tree.xpath(
            './/mets:fileGrp[@USE="OBJECTS"]//mets:FLocat/@xlink:href',
            namespaces=NAMESPACES,
        )
    )


def check_bag_references(bag_files, mets_xml, mets_name):
    alto_refs = get_alto_references(mets_xml)
    object_refs = get_object_references(mets_xml)

    missing_alto = alto_refs - set(bag_files.keys())
    if missing_alto:
        abort(
            f"METS file {mets_name} refers to ALTO files that are missing "
            f"from the bag: {', '.join(sorted(missing_alto))}"
        )

    missing_objects = object_refs - set(bag_files.keys())
    if missing_objects:
        abort(
            f"METS file {mets_name} refers to objects that are missing "
            f"from the bag: {', '.join(sorted(missing_objects))}"
        )

    info(f"{mets_name}: All ALTO and object references are correct")


if __name__ == "__main__":
    try:
        b_number = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <B_NUMBER>")

    client = get_storage_client(api_url="https://api.wellcomecollection.org/storage/v1")

    bag = client.get_bag(space_id="digitised", source_id=b_number)
    info(
        "Retrieved storage manifest for %s %s from the API" % (b_number, bag["version"])
    )

    bag_files = {
        f["name"][len("data/") :]: f
        for f in bag["manifest"]["files"]
        if f["name"].startswith("data/")
    }

    root_mets_name = f"{b_number}.xml"

    # Find the METS file in the bag.  This should always be in the top level
    # of the data directory, named after the bnumber,
    #
    # e.g. data/b30354730.xml
    #
    try:
        root_mets_file = bag_files[root_mets_name]
    except KeyError:
        abort(f"Unable to find root METS file {root_mets_name}")
    else:
        info(f"Found root METS file {root_mets_name} in bag")

    root_mets = get_xml_from_s3(location=bag["location"], path=root_mets_file["path"])
    info("Downloaded root METS file from S3")

    # If it's a multiple manifestation, we'd expect to see something like
    # (some attributes omitted):
    #
    #   <mets:structMap TYPE="LOGICAL">
    #     <mets:div TYPE="MultipleManifestation">
    #       <mets:div>
    #         <mets:mptr LOCTYPE="URL" xlink:href="b24748389_0001.xml" />
    #       </mets:div>
    #       <mets:div>
    #         <mets:mptr LOCTYPE="URL" xlink:href="b24748389_0002.xml" />
    #       </mets:div>
    #     </mets:div>
    #   </mets:structMap>
    #
    multiple_manifestations = root_mets.xpath(
        './/mets:div[@TYPE="MultipleManifestation"]//mets:mptr/@xlink:href',
        namespaces=NAMESPACES,
    )

    alto_refs = get_alto_references(root_mets)
    object_refs = get_object_references(root_mets)

    if multiple_manifestations and (alto_refs or object_refs):
        abort(
            "Something is wrong with the root METS file: it is an anchor file for "
            "other volumes *and* it refers to JP2 and ALTO files."
        )

    elif not (multiple_manifestations or alto_refs or object_refs):
        abort(
            "Something is wrong with the root METS file: it is not an anchor file, "
            "nor does it refer to any JP2 or ALTO files"
        )

    if multiple_manifestations:
        info("Multiple manifestation detected!")
        info(
            "The root METS file is an anchor file that refers to %d volume%s"
            % (
                len(multiple_manifestations),
                "s" if multiple_manifestations != 1 else "",
            )
        )

        for mets_name in multiple_manifestations:
            try:
                mets_bag_entry = bag_files[mets_name]
            except KeyError:
                abort(f"Unable to find METS file {mets_name} in bag")

            mets_volume = get_xml_from_s3(
                location=bag["location"], path=mets_bag_entry["path"]
            )
            check_bag_references(bag_files, mets_xml=mets_volume, mets_name=mets_name)
    else:
        check_bag_references(bag_files, mets_xml=root_mets, mets_name=root_mets_name)

    success(f"Bag {b_number} is formatted correctly 🎉")
