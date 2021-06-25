package weco.storage_service.bagit.models

case class Bag(
  info: BagInfo,
  manifest: PayloadManifest,
  tagManifest: TagManifest,
  fetch: Option[BagFetch] = None
)
