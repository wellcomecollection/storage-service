from requests import get
import settings


def get_package_file_modified(bnumber):
    url = settings.DDS_PACKAGE_FILEINFO.format(bnumber)
    info = get(url).json()
    if info.get("Exists", False):
        return info["LastWriteTime"]
    return None


def notify_dds_goobi_call(bnumber):
    url = settings.DDS_GOOBI_NOTIFICATION.format(bnumber)
    return get(url).json()


def get_text_info(bnumber):
    texts_expected = -1
    texts_cached = 0
    url = settings.DDS_TEXT_INFO.format(bnumber)
    info = get(url).json()
    if info.get("Exists", False):
        texts_expected = info["TextsExpected"]
        texts_cached = info["TextsCached"]
    return texts_expected, texts_cached


def get_dlcs_mismatch(bnumber):
    dlcs_mismatch = -1
    url = settings.DDS_DLCS_COUNTS.format(bnumber)
    info = get(url).json()
    if info.get("Exists", False):
        dlcs_mismatch = info["DlcsMismatch"]
    return dlcs_mismatch
