package weco.storage_service.bagit.models

import weco.storage_service.checksum.{ChecksumAlgorithm, ChecksumValue, MultiManifestChecksum}

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
  // and § 2.2.1 (https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.1):
  //
  //      Each tag manifest MUST NOT list any tag manifests but SHOULD list the
  //      remaining tag files present in the bag.
  //
  // This should already have been checked by the BagReader before creating
  // an instance of this class, so we can display useful errors to users.
  // This assertion is meant to prevent against programmer error in the
  // storage service code, not malformed bags uploaded by users.
  //
  require(
    entries.values.map(_.definedAlgorithms).toSeq.distinct.size == 1,
    "Different entries are using different algorithms!"
  )
}

case class NewPayloadManifest(entries: Map[BagPath, MultiManifestChecksum]) extends NewBagManifest

case class NewTagManifest(entries: Map[BagPath, MultiManifestChecksum]) extends NewBagManifest