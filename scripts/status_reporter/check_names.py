# -*- encoding: utf-8

STORAGE_MANIFESTS = "storage_manifest_created"

DDS_SYNC = "dds_sync_complete"

DLCS_ORIGIN_MATCH = "dlcs_origin_match"

IIIF_MANIFESTS_CONTENTS = "iiif_manifest_contents_match"

IIIF_MANIFESTS_FILE_SIZES = "iiif_manifest_file_sizes_match"

MANUAL_SKIP = "manual_skip"

ALL_CHECK_NAMES = [
    STORAGE_MANIFESTS,
    DDS_SYNC,
    IIIF_MANIFESTS_CONTENTS,
    IIIF_MANIFESTS_FILE_SIZES,
    DLCS_ORIGIN_MATCH,
    MANUAL_SKIP,
]
