# -*- encoding: utf-8

import os
import pathlib
import tempfile

from botocore.exceptions import ClientError
import pyodbc

from aws_client import read_only_client


def get_preservica_asset_path(guid):
    result = (
        _get_preservica_asset_path_from_s3(guid) or
        _get_preservica_asset_path_from_euston_road(guid)
    )

    if result is None:
        raise RuntimeError(
            f"Could not find asset {guid} in Preservica local storage or S3!"
        )

    return result


def get_preservica_asset_size(guid):
    db_size = _get_preservica_asset_size_from_db(guid)

    actual_size = _get_preservica_asset_size_from_s3(
        guid
    ) or _get_preservica_asset_size_from_euston_road(guid)

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
    return os.stat(euston_road_path).st_size


def _get_preservica_asset_size_from_s3(guid):
    s3_client = read_only_client.s3_client()

    try:
        head_resp = s3_client.head_object(Bucket="wdl-preservica", Key=guid)
    except ClientError as err:
        if err.args[0].startswith("An error occurred (404)"):
            return None
        else:
            raise
    else:
        return head_resp["ContentLength"]


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

    print(get_preservica_asset_path(guid), get_preservica_asset_size(guid))
