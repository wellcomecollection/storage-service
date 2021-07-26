package weco.storage_service.bagit.models

case class Bag(
  info: BagInfo,
  payloadManifest: PayloadManifest,
  tagManifest: TagManifest,
  fetch: Option[BagFetch]
) {

  // Quoting RFC 8493 § 2.2.1 (https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.1):
  //
  //      Tag manifests SHOULD use the same algorithms as the payload manifests
  //      that are present in the bag.
  //
  // This should already have been checked by the BagReader before creating
  // an instance of this class, so we can display useful errors to users.
  // This assertion is meant to prevent against programmer error in the
  // storage service code, not malformed bags uploaded by users.
  //
  require(
    payloadManifest.algorithms == tagManifest.algorithms,
    "Payload and tag manifests use different algorithms!"
  )
}
