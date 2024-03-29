package weco.storage_service.bagit.models

import weco.storage_service.checksum.{ChecksumAlgorithm, MultiManifestChecksum}

sealed trait BagManifest {
  val algorithms: Set[ChecksumAlgorithm]
  val entries: Map[BagPath, MultiManifestChecksum]

  // Check that every file is using the same set of algorithms.  It would be
  // an error if, say, one file had an MD5 and SHA-256 checksum and another file
  // just had a SHA-256 checksum.
  //
  // This is a consequence of RFC 8493 § 3 (https://datatracker.ietf.org/doc/html/rfc8493#section-3):
  //
  //      Every payload file MUST be listed in every payload manifest
  //
  // Note that the payload manifest might be empty, if there are no files in the bag,
  // which is why we ask the caller to supply a list of algorithms in use.
  //
  // This should already have been checked by the BagReader before creating
  // an instance of this class, so we can display useful errors to users.
  // This assertion is meant to prevent against programmer error in the
  // storage service code, not malformed bags uploaded by users.
  //
  require(
    entries.values.forall(_.definedAlgorithms.toSet == algorithms),
    s"Different manifest entries are using different algorithms!"
  )
}

case class PayloadManifest(
  algorithms: Set[ChecksumAlgorithm],
  entries: Map[BagPath, MultiManifestChecksum]
) extends BagManifest

case class TagManifest(
  algorithms: Set[ChecksumAlgorithm],
  entries: Map[BagPath, MultiManifestChecksum]
) extends BagManifest
