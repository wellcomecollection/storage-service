import pytest

from miro_ids import (
    parse_miro_id,
    IsCorporatePhotographyError,
    IsMiroMoviesError,
    NotMiroAssetError,
    UnknownMiroIDError,
)


@pytest.mark.parametrize(
    "s3_key", ["miro/Wellcome_Images_Archive/.apdisk", "Thumbs.db", ".DS_Store",]
)
def test_raises_not_miro_asset_error(s3_key):
    with pytest.raises(NotMiroAssetError):
        parse_miro_id(s3_key)


def test_spots_miro_movies():
    with pytest.raises(IsMiroMoviesError):
        parse_miro_id("miro/Wellcome_Images_Archive/Movies/N0038416.avi")


@pytest.mark.parametrize(
    "s3_key",
    [
        "miro/Wellcome_Images_Archive/Corporate_Photography/0904-1004/0919_Tricycle_Theatre/C0027049.NEF",
        "miro/Wellcome_Images_Archive/C Scanned/C0000000/C000066FB12.JP2",
        "miro/Wellcome_Images_Archive/A Images/A00036000/C0136679.JP2",
    ],
)
def test_spots_corporate_photography(s3_key):
    with pytest.raises(IsCorporatePhotographyError):
        parse_miro_id(s3_key)


@pytest.mark.parametrize(
    "s3_key, miro_id",
    [
        (
            "miro/Wellcome_Images_Archive/A Images/A0000000/A0000001-CS-LS.jp2",
            "A0000001",
        ),
        ("miro/Wellcome_Images_Archive/A Images/A0001000/A0001324.JP2", "A0001324"),
        (
            "miro/Wellcome_Images_Archive/AS Images/AS0000000/AS000000105-LS-LH.jp2",
            "AS000000105",
        ),
        (
            "miro/Wellcome_Images_Archive/AS Images/AS0000000/AS0000001F05.jp2",
            "AS0000001F05",
        ),
        (
            "miro/Wellcome_Images_Archive/B Images/B0000000/B0000001-LH-LH.jp2",
            "B0000001",
        ),
        ("miro/Wellcome_Images_Archive/B Images/B0000000/B0000103.tif", "B0000103"),
        ("miro/Wellcome_Images_Archive/L Images/L0056000/L0056873.dt", "L0056873"),
    ],
)
def test_parses_miro_id(s3_key, miro_id):
    assert parse_miro_id(s3_key) == miro_id
