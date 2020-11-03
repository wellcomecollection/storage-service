import os
import re


class UnknownMiroIDError(Exception):
    pass


class IgnoreMiroIDError(Exception):
    """
    Raised if this is the Miro ID of an object that we can ignore.
    """

    pass


class NotMiroAssetError(IgnoreMiroIDError):
    """
    Raised if you try to get the Miro ID from an object which isn't a Miro asset,
    e.g. .DS_Store or thumbs.db
    """

    pass


class IsMiroMoviesError(IgnoreMiroIDError):
    """
    Raised if you try to get the Miro ID from something in the Movies directory.
    """

    pass


class IsCorporatePhotographyError(IgnoreMiroIDError):
    """
    Raised if you try to get the Miro ID from somethign that's Editorial Photography.
    """

    pass


def parse_miro_id(s3_key):
    """
    Returns the Miro ID from an S3 key.
    """
    filename = os.path.basename(s3_key)

    # These are some keys with very odd filenames.  We should probably preserve
    # the original filename so all the metadata files match, but get
    # the numeric ID for our own processing purposes.
    # e.g. miro/Wellcome_Images_Archive/N Images/N0019000/NOO19209.jp2
    if filename.startswith("NOO"):
        filename = filename.replace("NOO", "N00")

    name, ext = os.path.splitext(filename)

    # We're going to ignore metadata files, so we don't need to parse a Miro ID
    # from them.  e.g. .apdisk, .DS_Store, Thumbs.db
    if name.startswith(".") or filename == "Thumbs.db":
        raise NotMiroAssetError(s3_key)

    # We haven't decided how we've going to categorise the Movies yet, so don't
    # bother trying to parse a Miro ID.
    # e.g. miro/Wellcome_Images_Archive/Movies/N0038416.avi
    elif s3_key.startswith("miro/Wellcome_Images_Archive/Movies/"):
        raise IsMiroMoviesError(s3_key)

    # Similarly corporate photography will be handled separately.
    elif s3_key.startswith(
        "miro/Wellcome_Images_Archive/Corporate_Photography/"
    ) or name.startswith(("C", "c", "Ç", "ç")):
        raise IsCorporatePhotographyError(s3_key)

    # e.g. A0000001-CS-LS.jp2
    elif ext in {".jp2", ".JP2", ".tif", ".dt", ".dt 2"}:
        return name.split("-")[0]

    raise UnknownMiroIDError(s3_key)
