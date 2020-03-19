package uk.ac.wellcome.platform.archive.common.bagit.models

case class Bag(
  info: BagInfo,
  manifest: PayloadManifest,
  tagManifest: TagManifest,
  fetch: Option[BagFetch] = None
)
