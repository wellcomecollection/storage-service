import os
import re


class UnknownMiroIDError(Exception):
    pass


class NotMiroAssetError(UnknownMiroIDError):
    """
    Raised if you try to get the Miro ID from an object which isn't a Miro asset,
    e.g. .DS_Store or thumbs.db
    """

    pass


class IsMiroMoviesError(UnknownMiroIDError):
    """
    Raised if you try to get the Miro ID from something in the Movies directory.
    """

    pass


class IsCorporatePhotographyError(UnknownMiroIDError):
    """
    Raised if you try to get the Miro ID from somethign that's Editorial Photography.
    """

    pass


def parse_miro_id(s3_key):
    """
    Returns the Miro ID from an S3 key.
    """
    filename = os.path.basename(s3_key)
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
    ) or name.startswith("C"):
        raise IsCorporatePhotographyError(s3_key)

    # e.g. A0000001-CS-LS.jp2
    elif ext in {".jp2", ".JP2", ".tif", ".dt", ".dt 2"}:
        return name.split("-")[0]

    raise UnknownMiroIDError(s3_key)
