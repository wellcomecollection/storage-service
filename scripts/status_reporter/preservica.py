# -*- encoding: utf-8

import collections
import os
import pathlib
import tempfile

from botocore.exceptions import ClientError
import pyodbc

from aws_client import read_only_client


def get_preservica_asset_path(guid):
    result = _get_preservica_asset_path_from_s3(
        guid
    ) or _get_preservica_asset_path_from_euston_road(guid)

    if result is None:
        raise RuntimeError(
            f"Could not find asset {guid} in Preservica local storage or S3!"
        )

    return result


LAST_CHECKS = collections.deque([], maxlen=5)


def get_preservica_asset_size(guid):
    db_size = _get_preservica_asset_size_from_db(guid)

    # The aim of this slightly fiddly looking code is to bias the Preservica
    # size lookup.  All the files in a single bag will typically be in the
    # same place: all in S3 or all in Euston Road.
    #
    # If this function is called repeatedly, it keeps track of the last few
    # places it was asked to read a file from, and uses that to decide
    # where to look next.
    #
    # At worst, we do an unnecessary filesystem call or S3 lookup, at best
    # this function goes a bit faster.
    if LAST_CHECKS.count("s3") > 3:
        s3_size = _get_preservica_asset_size_from_s3(guid)
        if s3_size is None:
            actual_size = _get_preservica_asset_size_from_euston_road(guid)
            LAST_CHECKS.append("euston_rd")
        else:
            actual_size = s3_size
            LAST_CHECKS.append("s3")
    else:
        euston_rd_size = _get_preservica_asset_size_from_euston_road(guid)
        if euston_rd_size is None:
            actual_size = _get_preservica_asset_size_from_s3(guid)
            LAST_CHECKS.append("s3_size")
        else:
            actual_size = euston_rd_size
            LAST_CHECKS.append("euston_rd")

    if actual_size is None:
        raise RuntimeError(
            f"Could not find asset {guid} in Preservica local storage or S3!"
        )

    assert db_size == actual_size

    return db_size


def get_preservica_cursor():
    cnxn = pyodbc.connect(
        driver="{ODBC Driver 17 for SQL Server}",
        server="wt-dhaka",
        database="WELLCOME_SDB4",
        trusted_connection="yes",
    )

    return cnxn.cursor()


def _get_preservica_asset_path_from_euston_road(guid):
    cursor = get_preservica_cursor()

    cursor.execute(
        """
        SELECT File_Path AS FilePath, ManifestationRef FROM File_Location
        INNER JOIN ManifestationFile ON File_Ref=FileRef WHERE File_Ref=?
    """,
        guid,
    )

    filepath, manifestation_ref = cursor.fetchone()

    return (
        pathlib.Path("/Volumes/LIB_WDL_SDB_STORE001")
        / manifestation_ref[:2]
        / manifestation_ref[2:4]
        / manifestation_ref[4:6]
        / manifestation_ref
        / filepath.replace("\\", "/")
    )


def _get_preservica_asset_size_from_db(guid):
    cursor = get_preservica_cursor()

    cursor.execute("""SELECT FileSize FROM DigitalFile WHERE FileRef=?""", guid)

    file_size, = cursor.fetchone()

    return file_size


def _get_preservica_asset_size_from_euston_road(guid):
    euston_road_path = _get_preservica_asset_path_from_euston_road(guid)
    try:
        return os.stat(euston_road_path).st_size
    except FileNotFoundError:
        pass


KNOWN_SIZES = {}


def _get_preservica_asset_size_from_s3(guid):
    # To save us making loads of HeadObject requests, this function maintains
    # a small internal cache of object sizes in S3.
    try:
        return KNOWN_SIZES[guid]
    except KeyError:
        s3_client = read_only_client.s3_client()

        resp = s3_client.list_objects_v2(
            Bucket="wdl-preservica",

            # Chop a few chars off the end so we get the GUID we're looking for,
            # if it's here.
            StartAfter=guid[:-2]
        )

        for s3_obj in resp["Contents"]:
            KNOWN_SIZES[s3_obj["Key"]] = s3_obj["Size"]

        return KNOWN_SIZES.get(guid)


def _get_preservica_asset_path_from_s3(guid):
    """
    Try to download a Preservica asset from the wdl-preservica bucket in S3.
    """
    s3_client = read_only_client.s3_client()
    _, path = tempfile.mkstemp()

    try:
        s3_client.download_file(Bucket="wdl-preservica", Key=guid, Filename=path)
    except ClientError as err:
        if err.args[0].startswith("An error occurred (404)"):
            return None
        else:
            raise
    else:
        return pathlib.Path(path)


if __name__ == "__main__":
    import sys

    try:
        guid = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <PRESERVICA_GUID>")

    print(get_preservica_asset_size(guid))
