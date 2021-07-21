package weco.storage_service.bagit.models

import weco.storage_service.checksum.{
  ChecksumAlgorithm,
  ChecksumValue,
  MultiManifestChecksum
}

sealed trait BagManifest {
  val checksumAlgorithm: ChecksumAlgorithm
  val entries: Map[BagPath, ChecksumValue]

  def paths: Seq[BagPath] = entries.keys.toSeq
}

case class PayloadManifest(
  checksumAlgorithm: ChecksumAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest

case class TagManifest(
  checksumAlgorithm: ChecksumAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest

sealed trait NewBagManifest {
  val algorithmsInUse: Set[ChecksumAlgorithm]
  val entries: Map[BagPath, MultiManifestChecksum]

  def paths: Seq[BagPath] = entries.keys.toSeq

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
    entries.values.forall(_.definedAlgorithms.toSet == algorithmsInUse),
    s"Different manifest entries are using different algorithms!"
  )
}

case class NewPayloadManifest(
  algorithmsInUse: Set[ChecksumAlgorithm],
  entries: Map[BagPath, MultiManifestChecksum])
    extends NewBagManifest

case class NewTagManifest(
  algorithmsInUse: Set[ChecksumAlgorithm],
  entries: Map[BagPath, MultiManifestChecksum])
    extends NewBagManifest
