# -*- encoding: utf-8

METS_EXISTS = "s3_mets_exists"
STORAGE_MANIFESTS = "storage_manifest_created"
DDS_SYNC = "dds_sync_complete"
IIIF_MANIFESTS_CONTENTS = "iiif_manifest_contents_match"
IIIF_MANIFESTS_FILE_SIZES = "iiif_manifest_file_sizes_match"
DLCS_ORIGIN_MATCH = "dlcs_origin_match"
DLCS_THUMBNAIL_RETRIEVE = "dlcs_thumbnail_retrieve"
DLCS_ORIGIN_RETRIEVE = "dlcs_origin_retrieve"

MANUAL_SKIP = "manual_skip"

ALL_CHECK_NAMES = [
    STORAGE_MANIFESTS,
    DDS_SYNC,
    IIIF_MANIFESTS_CONTENTS,
    IIIF_MANIFESTS_FILE_SIZES,
    DLCS_ORIGIN_MATCH,
    MANUAL_SKIP,
    METS_EXISTS,
    DLCS_THUMBNAIL_RETRIEVE,
    DLCS_ORIGIN_RETRIEVE,
]
