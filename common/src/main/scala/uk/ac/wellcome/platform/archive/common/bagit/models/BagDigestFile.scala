package uk.ac.wellcome.platform.archive.common.bagit.models

case class BagDigestFile(
  checksum: String,
  path: BagItemPath
)
